package rafted

import (
    "errors"
)

type CommitCondition interface {
    AddVote(net.Addr) error
    IsCommitted() bool
}

type MajorityCommitCondition struct {
    VoteCount    uint32
    ClusterSize  uint32
    MajoritySize uint32
}

func NewMajorityCommitCondition(
    clusterSize uint32) *MajorityCommitCondition {

    return &MajorityCommitCondition{
        VoteCount:    0,
        ClusterSize:  clusterSize,
        MajoritySize: (clusterSize / 2) + 1,
    }
}

func (self *MajorityCommitCondition) AddVote(_ net.Addr) error {
    if self.VoteCount < self.ClusterSize {
        self.VoteCount++
        return nil
    }
    return errors.New("vote overflow")
}

func (self *MajorityCommitCondition) IsCommitted() bool {
    return self.VoteCount >= self.MajoritySize
}

type MemberChangeCommitCondition struct {
    OldServersVoteStatus      map[net.Addr]bool
    OldServersCommitCondition *MajorityCommitCondition
    NewServersVoteStatus      map[net.Addr]bool
    NewServersCommitCondition *MajorityCommitCondition
}

func NewMemberChangeCommitCondition(conf *Config) *MemoryChangeCommitCondition {
    genVoteStatus := func(servers []net.Addr) map[net.Addr]bool {
        voteStatus := make(map[net.Addr]bool)
        for _, server := range conf.Servers {
            voteStatus[server] = false
        }
        return voteStatus
    }
    oldServersLen := len(conf.Servers)
    newServersLen := len(conf.NewServers)
    return &MemoryChangeCommitCondition{
        OldServersVoteStatus:      genVoteStatus(conf.Servers),
        OldServersCommitCondition: NewMajorityCommitCondition(oldServersLen),
        NewServersVoteStatus:      genVoteStatus(conf.NewServers),
        NewServersCommitCondition: NewMajorityCommitCondition(NewServersLen),
    }
}

func (self *MemberChangeCommitCondition) AddVote(addr net.Addr) error {
    voteSuccess := false
    if _, ok := self.OldServersVoteStatus[addr]; ok {
        if err := self.OldServersCommitCondition.AddVote(addr); err != nil {
            return err
        }
        self.OldServersVoteStatus[addr] = true
        voteSuccess = true
    }
    if _, ok := self.NewServersVoteStatus[addr]; ok {
        if err := self.NewServersCommitCondition.AddVote(addr); err != nil {
            return err
        }
        self.NewServersVoteStatus[addr] = true
        voteSuccess = true
    }
    if voteSuccess {
        return nil
    }
    return errors.New(
        fmt.Sprintf("unknown server with addr: %s", addr.String()))
}

func (self *MemberChangeCommitCondition) IsCommitted() bool {
    return (self.OldServersCommitCondition.IsCommitted() &&
        self.NewServersCommitCondition.IsCommitted())
}

type InflightRequest struct {
    LogType    persist.LogType
    Data       []byte
    Conf       *persist.Config
    ResultChan chan ev.ClientEvent
}

func (self *InflightRequest) SendResponse(event ev.ClientEvent) {
    self.ResultChan <- event
}

type InflightEntry struct {
    LogIndex  uint64
    Request   *InflightRequest
    Condition CommitCondition
}

func NewInflightEntry(
    logIndex uint64,
    request *InflightRequest) *InflightEntry {

    if request.LogType == persist.LogMemberChange {
        return &InfligthEntry{
            LogIndex:  logIndex,
            Request:   request,
            Condition: NewMemberChangeCommitCondition(request.Conf),
        }
    }
    return &InflightEntry{
        LogIndex:  logIndex,
        Request:   request,
        Condition: NewMajorityCommitCondition(len(request.Conf.Servers)),
    }
}

type Inflight struct {
    MinIndex           uint64
    MaxIndex           uint64
    ToCommitEntries    []*InflightEntry
    CommittedEntries   []*InflightEntry
    ServerMatchIndexes map[net.Addr]uint64

    sync.Mutex
}

func setupServerMatchIndexes(
    conf *persist.Config, prev map[net.Addr]uint64) map[net.Addr]uint64 {

    serverMatchIndexes := make(map[net.Addr]uint64)
    initIndex := func(m map[net.Addr]uint64, addrs []net.Addr) {
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

func NewInflight(conf *persist.Config) *Inflight {
    matchIndexes := setupServerMatchIndexes(conf, make(map[net.Addr]uint64))
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

func (self *Inflight) ChangeMember(conf *persist.Config) {
    self.Lock()
    defer self.Unlock()

    newMatchIndexes := setupServerMatchIndexes(conf, self.ServerMatchIndexes)
    self.ServerMatchIndexes = newMatchIndexes
}

func (self *Inflight) Add(logIndex uint64, request *InflightRequest) error {
    self.Lock()
    defer self.Unlock()

    if logIndex <= self.MaxIndex {
        return errors.New("log index should be incremental")
    }

    self.MaxIndex = logIndex
    if self.MinIndex == 0 {
        self.MinIndex = logIndex
    }

    toCommit := NewInflightEntry(logIndex, request, self.ClusterSize)
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
        if !(index < entry.LogIndex) {
            return errors.New("log index should be incremental")
        }
        index = entry.LogIndex
    }

    self.MaxIndex = index
    if self.MinIndex == 0 {
        self.MinIndex = toCommits[0].LogIndex
    }
    self.ToCommitEntries = append(self.ToCommitEntries, toCommits...)
    return nil
}

func (self *Inflight) Replicate(
    addr net.Addr, newMatchIndex uint64) (bool, error) {

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
        if toCommit.LogIndex > newMatchIndex {
            break
        }
        if toCommit.LogIndex <= matchIndex {
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
        if toCommit.LogIndex > newMatchIndex {
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
