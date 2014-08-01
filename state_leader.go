package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "sync"
)

type LeaderState struct {
    *hsm.StateHead

    // TODO add fields
    Peers    []net.Addr
    Inflight *Inflight
}

func NewLeaderState(super hsm.State, peers []net.Addr) *LeaderState {
    object := &LeaderState{
        StateHead: hsm.NewStateHead(super),
        Peers:     peers,
        Inflight:  NewInflight(peers),
    }
    super.AddChild(object)
    return object
}

func (*LeaderState) ID() string {
    return StateLeaderID
}

func (self *LeaderState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    // init global status
    raftHSM.SetLeader(raftHSM.LocalAddr)
    // init status for this state
    self.Inflight.Init()
    return nil
}

func (self *LeaderState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StateSyncID)
    return nil
}

func (self *LeaderState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // cleanup status for this state
    self.Inflight.Init()
    return nil
}

func (self *LeaderState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event=", event)
    switch {
    case event.Type() == ev.EventPeerReplicateLog:
        e, ok := event.(*ev.PeerReplicateLogEvent)
        hsm.AssertTrue(ok)
        goodToCommit, err := self.Inflight.Replicate(e.Peer, e.MatchIndex)
        if err != nil {
            // TODO add log
            return nil
        }
        if goodToCommit {
            allCommitted := self.Inflight.Committed()
            self.CommitInflightEntries(raftHSM, allCommitted)
        }
        return nil
    case event.Type() == ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        peerUpdate := ev.PeerReplicateLog{
            Peer:       raftHSM.LocalAddr,
            MatchIndex: e.Response.LastLogIndex,
        }
        self.SelfDispatch(ev.NewPeerReplicateLogEvent(peerUpdate))
        return nil
    case event.Type() == ev.EventRequestVoteRequest:
        // TODO DEBUG
        fmt.Println("Leader possible step down")
        return nil
    case ev.IsClientEvent(event.Type()):
        fmt.Println("Leader process request")
        return nil
    }
    return self.Super()
}

func (self *LeaderState) StartFlight(
    raftHSM *RaftHSM, requests []*InflightRequest) error {

    term := raftHSM.GetCurrentTerm()
    lastLogIndex := raftHSM.GetLog().GetLastIndex()

    // construct log entries and inflight log entries
    logEntries := make([]*persist.LogEntry, 0, len(requests))
    inflightEntries := make([]*InflightEntry, 0, len(requests))
    for index, request := range requests {
        logIndex := lastLogIndex + 1 + index
        logEntry := &persist.LogEntry{
            Term:  term,
            Index: logIndex,
            Type:  request.LogType,
            Data:  request.Data,
        }
        logEntries = append(logEntries, logEntry)

        inflightEntry := NewInflight(logIndex, request, self.ClusterSize)
        inflightEntries = append(inflightEntries, inflightEntry)
    }

    // persist these requests locally
    if err := raftHSM.GetLog().StoreLogs(logEntries); err != nil {
        // TODO error handling; add log
        response := &persist.ClientResponse{
            Success: false,
            Data:    make([]byte, 0),
        }
        event := persist.NewClientResponseEvent(response)
        for _, request := range requests {
            request.SendResponse(event)
        }
        return error
    }

    // add these requests to inflight log
    self.Inflight.AddAll(inflightEntries)

    // send AppendEntriesRequest to all peer
    leader, err := EncodeAddr(raftHSM.LocalAddr)
    if err != nil {
        // TODO add error handling
        return err
    }
    prevLogTerm, prevLogIndex := raftHSM.GetLastLogInfo()
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            leader,
        PrevLogTerm:       prevLogTerm,
        PrevLogIndex:      prevLogIndex,
        Entries:           logEntries,
        LeaderCommitIndex: raftHSM.GetCommitIndex(),
    }
    event := ev.NewAppendEntriesRequestEvent(request)

    selfResponse := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: prevLogIndex,
        Success:      true,
    }
    raftHSM.SelfDispatch(ev.NewAppendEntriesResponseEvent(selfResponse))
    raftHSM.PeerManager.Broadcast(event)
    return nil
}

func (self *LeaderState) CommitInflightEntries(
    raftHSM *RaftHSM, entries []*InflightEntry) {

    for _, entry := range entries {
        result := raftHSM.ProcessLogAt(entry.LogIndex)
        response := &ev.ClientResponse{
            Success: true,
            Data:    result,
        }
        entry.Request.SendResponse(ev.NewClientResponseEvent(response))
    }
}

type CommitCondition interface {
    AddVote()
    IsCommitted() bool
}

type MajorityCommitCondition struct {
    VoteCount    uint32
    ClusterSize  uint32
    MajoritySize uint32
}

func NewMajorityCommitCondition(clusterSize uint32) {
    return &MajorityCommitCondition{
        VoteCount:    0,
        ClusterSize:  clusterSize,
        MajoritySize: (ClusterSize / 2) + 1,
    }
}

func (self *MajorityCommitCondition) AddVote() {
    if self.VoteCount < self.ClusterSize {
        self.VoteCount++
    }
}

func (self *MajorityCommitCondition) IsCommitted() bool {
    return self.VoteCount >= self.MajoritySize
}

type InflightRequest struct {
    LogType    persist.LogType
    Data       []byte
    ResultChan chan persist.ClientEvent
}

func (self *InflightRequest) SendResponse(event persist.ClientEvent) {
    self.ResultChan <- event
}

type InflightEntry struct {
    LogIndex  uint64
    Request   *InflightRequest
    Condition CommitCondition
}

func NewInflightEntry(
    logIndex uint64,
    request *InflightRequest,
    clusterSize uint32) *InflightEntry {

    return &InflightEntry{
        LogIndex:  logIndex,
        Request:   request,
        Condition: NewMajorityCommitCondition(clusterSize),
    }
}

type Inflight struct {
    MinIndex         uint64
    MaxIndex         uint64
    ToCommit         []*InflightEntry
    Committed        []*InflightEntry
    ClusterSize      uint32
    PeerMatchIndexes map[net.Addr]uint64

    sync.Mutex
}

func NewInflight(peers []net.Addr) *inflight {
    peerMatchIndexes := make(map[net.Addr]uint64)
    for _, peer := range peers {
        peerMatchIndexes[peer] = 0
    }
    return &inflight{
        MinIndex:         0,
        MaxIndex:         0,
        ToCommit:         make([]*InflightEntry, 0),
        Committed:        make([]*InflightEntry, 0),
        ClusterSize:      uint32(len(peers)),
        PeerMatchIndexes: peerMatchIndexes,
    }
}

func (self *Inflight) Init() {
    self.MinIndex = 0
    self.MaxIndex = 0
    self.ToCommit = make([]*InflightEntry, 0)
    self.Committed = make([]*InflightEntry, 0)
    for k, _ := range self.PeerMatchIndexes {
        self.PeerMatchIndexes[k] = 0
    }
}

func (self *Inflight) Add(logIndex uint64, request InflightRequest) error {
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
    self.ToCommit = append(self.ToCommit, toCommit)
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
    for toCommit := range toCommits {
        if !(index < toCommit.LogIndex) {
            return errors.New("log index should be incremental")
        }
        index = toCommit.LogIndex
    }

    self.MaxIndex = index
    if self.MinIndex == 0 {
        self.MinIndex = toCommits[0].Index
    }
    self.ToCommit = append(self.ToCommit, toCommits...)
    return nil
}

func (self *Inflight) Replicate(
    peer net.Addr, newMatchIndex uint64) (bool, error) {

    self.Lock()
    defer self.Unlock()

    // health check
    matchIndex, ok := self.PeerMatchIndexes[peer]
    if !ok {
        return false, errors.New(fmt.Sprintf("unknown peer %#v", peer))
    }
    if matchIndex >= newMatchIndex {
        return false, errors.New(
            fmt.Sprintf("invalid new match index %#v, not greater than %#v",
                newMatchIndex, matchIndex))
    }
    // update vote count for new replicated log
    for _, toCommit := range self.ToCommit {
        if toCommit.LogIndex > newMatchIndex {
            break
        }
        if toCommit.LogIndex <= matchIndex {
            continue
        }
        toCommit.Condition.AddVote()
    }

    // update match index for the specified peer
    self.PeerMatchIndexes[peer] = newMatchIndex

    // only inflight requests with log index up to(including)
    // newMatchIndex are possible to be good to commit
    committedIndex = 0
    for _, toCommit := range self.ToCommit {
        if toCommit.LogIndex > newMatchIndex {
            break
        }
        if !toCommit.Condition.IsCommitted() {
            break
        }
        committedIndex++
    }
    if committedIndex > 0 {
        self.Committed = append(
            self.Committed, self.ToCommit[:committedIndex]...)
        self.ToCommit = self.ToCommit[committedIndex:]
        return true, nil
    } else {
        return false, nil
    }
}

func (self *Inflight) Committed() []*InflightEntry {
    self.Lock()
    defer self.Unlock()

    committed := self.Committed
    self.Committed = make([]*InflightEntry, 0)
    return committed
}
