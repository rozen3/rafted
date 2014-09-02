package rafted

import (
    "errors"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "sync"
)

type CommitCondition interface {
    AddVote(ps.ServerAddr) error
    IsCommitted() bool
}

type MajorityCommitCondition struct {
    VoteStatus   map[ps.ServerAddr]bool
    VoteCount    uint32
    MajoritySize uint32
}

func NewMajorityCommitCondition(
    addrs []ps.ServerAddr) *MajorityCommitCondition {

    voteStatus := make(map[ps.ServerAddr]bool)
    for _, addr := range addrs {
        voteStatus[addr] = false
    }
    return &MajorityCommitCondition{
        VoteStatus:   voteStatus,
        VoteCount:    0,
        MajoritySize: uint32((len(addrs) / 2) + 1),
    }
}

func (self *MajorityCommitCondition) AddVote(addr ps.ServerAddr) error {
    if _, ok := self.VoteStatus[addr]; ok {
        return errors.New(fmt.Sprintf("%s already voted", addr.String()))
    }
    self.VoteStatus[addr] = true
    self.VoteCount++
    return nil
}

func (self *MajorityCommitCondition) IsInCluster(addr ps.ServerAddr) bool {
    if _, ok := self.VoteStatus[addr]; ok {
        return true
    }
    return false
}

func (self *MajorityCommitCondition) IsCommitted() bool {
    return self.VoteCount >= self.MajoritySize
}

type MemberChangeCommitCondition struct {
    OldServersCommitCondition *MajorityCommitCondition
    NewServersCommitCondition *MajorityCommitCondition
}

func NewMemberChangeCommitCondition(
    conf *ps.Config) *MemberChangeCommitCondition {

    return &MemberChangeCommitCondition{
        OldServersCommitCondition: NewMajorityCommitCondition(conf.Servers),
        NewServersCommitCondition: NewMajorityCommitCondition(conf.NewServers),
    }
}

func (self *MemberChangeCommitCondition) AddVote(addr ps.ServerAddr) error {
    if self.OldServersCommitCondition.IsInCluster(addr) {
        if err := self.OldServersCommitCondition.AddVote(addr); err != nil {
            return err
        }
        return nil
    }
    if self.NewServersCommitCondition.IsInCluster(addr) {
        if err := self.NewServersCommitCondition.AddVote(addr); err != nil {
            return err
        }
        return nil
    }
    return errors.New(
        fmt.Sprintf("addr %s not in old/new config", addr.String()))
}

func (self *MemberChangeCommitCondition) IsCommitted() bool {
    return (self.OldServersCommitCondition.IsCommitted() &&
        self.NewServersCommitCondition.IsCommitted())
}

type InflightRequest struct {
    LogEntry   *ps.LogEntry
    ResultChan chan ev.RaftEvent
}

type InflightEntry struct {
    Request   *InflightRequest
    Condition CommitCondition
}

func NewInflightEntry(request *InflightRequest) *InflightEntry {
    if ps.IsNormalConfig(request.LogEntry.Conf) {
        return &InflightEntry{
            Request: request,
            Condition: NewMajorityCommitCondition(
                request.LogEntry.Conf.Servers),
        }
    }
    return &InflightEntry{
        Request:   request,
        Condition: NewMemberChangeCommitCondition(request.LogEntry.Conf),
    }
}

type Inflight struct {
    MinIndex           uint64
    MaxIndex           uint64
    ToCommitEntries    []*InflightEntry
    CommittedEntries   []*InflightEntry
    ServerMatchIndexes map[ps.ServerAddr]uint64

    sync.Mutex
}

func setupServerMatchIndexes(
    conf *ps.Config, prev map[ps.ServerAddr]uint64) map[ps.ServerAddr]uint64 {

    serverMatchIndexes := make(map[ps.ServerAddr]uint64)
    initIndex := func(m map[ps.ServerAddr]uint64, addrs []ps.ServerAddr) {
        for _, addr := range addrs {
            serverMatchIndexes[addr] = 0
        }
    }
    if conf.Servers != nil {
        initIndex(serverMatchIndexes, conf.Servers)
    }
    if conf.NewServers != nil {
        initIndex(serverMatchIndexes, conf.NewServers)
    }

    for addr, _ := range serverMatchIndexes {
        if matchIndex, ok := prev[addr]; ok {
            serverMatchIndexes[addr] = matchIndex
        }
    }
    return serverMatchIndexes
}

func NewInflight(conf *ps.Config) *Inflight {
    defaultValue := make(map[ps.ServerAddr]uint64)
    matchIndexes := setupServerMatchIndexes(conf, defaultValue)
    return &Inflight{
        MinIndex:           0,
        MaxIndex:           0,
        ToCommitEntries:    make([]*InflightEntry, 0),
        CommittedEntries:   make([]*InflightEntry, 0),
        ServerMatchIndexes: matchIndexes,
    }
}

func (self *Inflight) Init() {
    self.Lock()
    defer self.Unlock()

    self.MinIndex = 0
    self.MaxIndex = 0
    self.ToCommitEntries = make([]*InflightEntry, 0)
    self.CommittedEntries = make([]*InflightEntry, 0)
    for k, _ := range self.ServerMatchIndexes {
        self.ServerMatchIndexes[k] = 0
    }
}

func (self *Inflight) ChangeMember(conf *ps.Config) {
    self.Lock()
    defer self.Unlock()

    newMatchIndexes := setupServerMatchIndexes(conf, self.ServerMatchIndexes)
    self.ServerMatchIndexes = newMatchIndexes
}

func (self *Inflight) Add(request *InflightRequest) error {
    self.Lock()
    defer self.Unlock()

    logIndex := request.LogEntry.Index
    if logIndex <= self.MaxIndex {
        return errors.New("log index should be incremental")
    }

    self.MaxIndex = logIndex
    if self.MinIndex == 0 {
        self.MinIndex = logIndex
    }

    toCommit := NewInflightEntry(request)
    self.ToCommitEntries = append(self.ToCommitEntries, toCommit)
    return nil
}

func (self *Inflight) AddAll(toCommits []*InflightEntry) error {
    self.Lock()
    defer self.Unlock()

    if len(toCommits) == 0 {
        return errors.New("no inflight request to add")
    }

    // check the indexes are incremental
    index := self.MaxIndex
    for _, entry := range toCommits {
        if !(index < entry.Request.LogEntry.Index) {
            return errors.New("log index should be incremental")
        }
        index = entry.Request.LogEntry.Index
    }

    self.MaxIndex = index
    if self.MinIndex == 0 {
        self.MinIndex = toCommits[0].Request.LogEntry.Index
    }
    self.ToCommitEntries = append(self.ToCommitEntries, toCommits...)
    return nil
}

func (self *Inflight) Replicate(
    addr ps.ServerAddr, newMatchIndex uint64) (bool, error) {

    self.Lock()
    defer self.Unlock()

    // health check
    matchIndex, ok := self.ServerMatchIndexes[addr]
    if !ok {
        return false, errors.New(fmt.Sprintf("unknown address %#v", addr))
    }
    if matchIndex >= newMatchIndex {
        return false, errors.New(
            fmt.Sprintf("invalid new match index %#v, not greater than %#v",
                newMatchIndex, matchIndex))
    }
    // update vote count for new replicated log
    for _, toCommit := range self.ToCommitEntries {
        if toCommit.Request.LogEntry.Index > newMatchIndex {
            break
        }
        if toCommit.Request.LogEntry.Index <= newMatchIndex {
            continue
        }
        toCommit.Condition.AddVote(addr)
    }

    // update match index for the specified server
    self.ServerMatchIndexes[addr] = newMatchIndex

    // only inflight requests with log index up to(including)
    // newMatchIndex are possible to be good to commit
    committedIndex := 0
    for _, toCommit := range self.ToCommitEntries {
        if toCommit.Request.LogEntry.Index > newMatchIndex {
            break
        }
        if !toCommit.Condition.IsCommitted() {
            break
        }
        committedIndex++
    }
    if committedIndex > 0 {
        self.CommittedEntries = append(
            self.CommittedEntries, self.ToCommitEntries[:committedIndex]...)
        self.ToCommitEntries = self.ToCommitEntries[committedIndex:]
        return true, nil
    } else {
        return false, nil
    }
}

func (self *Inflight) GetCommitted() []*InflightEntry {
    self.Lock()
    defer self.Unlock()

    committed := self.CommittedEntries
    self.CommittedEntries = make([]*InflightEntry, 0)
    return committed
}
