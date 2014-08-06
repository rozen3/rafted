package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "github.com/hhkbp2/rafted/persist"
    "net"
    "sync"
)

type LeaderState struct {
    *hsm.StateHead

    Inflight *Inflight
}

func NewLeaderState(super hsm.State) *LeaderState {
    object := &LeaderState{
        StateHead: hsm.NewStateHead(super),
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
    // TODO init inflight
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
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventPeerReplicateLog:
        e, ok := event.(*ev.PeerReplicateLogEvent)
        hsm.AssertTrue(ok)
        goodToCommit, err := self.Inflight.Replicate(
            e.Message.Peer, e.Message.MatchIndex)
        if err != nil {
            // TODO add log
            return nil
        }
        if goodToCommit {
            allCommitted := self.Inflight.GetCommitted()
            self.CommitInflightEntries(raftHSM, allCommitted)
        }
        return nil
    case ev.EventStepDown:
        // TODO add impl
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        peerUpdate := &ev.PeerReplicateLog{
            Peer:       raftHSM.LocalAddr,
            MatchIndex: e.Response.LastLogIndex,
        }
        raftHSM.SelfDispatch(ev.NewPeerReplicateLogEvent(peerUpdate))
        return nil
    case ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        // step down to follower state if local term is not greater than
        // the remote one
        if e.Request.Term >= raftHSM.GetCurrentTerm() {
            Stepdown(raftHSM, event, e.Request.Leader)
        }
        return nil
    case ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        if e.Request.Term >= raftHSM.GetCurrentTerm() {
            Stepdown(raftHSM, event, e.Request.Candidate)
        }
        return nil
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        if e.Request.Term >= raftHSM.GetCurrentTerm() {
            Stepdown(raftHSM, event, e.Request.Leader)
        }
        return nil
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(*ev.ClientReadOnlyRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(
            raftHSM, e.Request.Data, e.ClientRequestEventHead.ResultChan)
        return nil
    case ev.EventClientWriteRequest:
        e, ok := event.(*ev.ClientWriteRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(
            raftHSM, e.Request.Data, e.ClientRequestEventHead.ResultChan)
        return nil
    }
    return self.Super()
}

func (self *LeaderState) HandleClientRequest(
    raftHSM *RaftHSM, requestData []byte, resultChan chan ev.ClientEvent) {

    requests := []*InflightRequest{
        &InflightRequest{
            LogType:    persist.LogCommand,
            Data:       requestData,
            ResultChan: resultChan,
        },
    }
    if err := self.StartFlight(raftHSM, requests); err != nil {
        // TODO error handling
    }
}

func (self *LeaderState) StartFlight(
    raftHSM *RaftHSM, requests []*InflightRequest) error {

    term := raftHSM.GetCurrentTerm()
    lastLogIndex, err := raftHSM.GetLog().LastIndex()
    if err != nil {
        // TODO add error handling
    }

    // construct log entries and inflight log entries
    logEntries := make([]*persist.LogEntry, 0, len(requests))
    inflightEntries := make([]*InflightEntry, 0, len(requests))
    for index, request := range requests {
        logIndex := lastLogIndex + 1 + uint64(index)
        logEntry := &persist.LogEntry{
            Term:  term,
            Index: logIndex,
            Type:  request.LogType,
            Data:  request.Data,
        }
        logEntries = append(logEntries, logEntry)

        inflightEntry := NewInflightEntry(logIndex, request, self.Inflight.ClusterSize)
        inflightEntries = append(inflightEntries, inflightEntry)
    }

    // persist these requests locally
    if err := raftHSM.GetLog().StoreLogs(logEntries); err != nil {
        // TODO error handling; add log
        response := &ev.ClientResponse{
            Success: false,
            Data:    make([]byte, 0),
        }
        event := ev.NewClientResponseEvent(response)
        for _, request := range requests {
            request.SendResponse(event)
        }
        return err
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

type UnsyncState struct {
    *hsm.StateHead

    noop     *InflightRequest
    listener *ClientEventListener
}

func NewUnsyncState(super hsm.State) *UnsyncState {
    ch := make(chan ev.ClientEvent, 1)
    object := &UnsyncState{
        StateHead: hsm.NewStateHead(super),
        noop: &InflightRequest{
            LogType:    persist.LogNoop,
            Data:       make([]byte, 0),
            ResultChan: ch,
        },
        listener: NewClientEventListener(ch),
    }
    super.AddChild(object)
    return object
}

func (*UnsyncState) ID() string {
    return StateUnsyncID
}

func (self *UnsyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    handleNoopResponse := func(event ev.ClientEvent) {
        if event.Type() != ev.EventClientResponse {
            // TODO add log
            return
        }
        raftHSM.SelfDispatch(event)
    }
    self.listener.Start(handleNoopResponse)
    self.StartSyncSafe(raftHSM)
    return nil
}

func (self *UnsyncState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    return self.Super()
}

func (self *UnsyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    self.listener.Stop()
    return nil
}

func (self *UnsyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.LeaderUnsyncResponse{}
        e.SendResponse(ev.NewLeaderUnsyncResponseEvent(response))
        return nil
    case event.Type() == ev.EventClientResponse:
        e, ok := event.(*ev.ClientResponseEvent)
        hsm.AssertTrue(ok)
        // TODO add different policy for retry
        if e.Response.Success {
            raftHSM.QTran(StateSyncID)
        } else {
            self.StartSyncSafe(raftHSM)
        }
        return nil
    }
    return self.Super()
}

func (self *UnsyncState) StartSync(raftHSM *RaftHSM) error {
    // commit a blank no-op entry into the log at the start of leader's term
    requests := []*InflightRequest{self.noop}
    leaderState, ok := self.Super().(*LeaderState)
    hsm.AssertTrue(ok)
    return leaderState.StartFlight(raftHSM, requests)
}

func (self *UnsyncState) StartSyncSafe(raftHSM *RaftHSM) {
    if err := self.StartSync(raftHSM); err != nil {
        // TODO add log
        stepdown := &ev.Stepdown{}
        raftHSM.SelfDispatch(ev.NewStepdownEvent(stepdown))
    }
}

type SyncState struct {
    *hsm.StateHead

    // TODO add fields
}

func NewSyncState(super hsm.State) *SyncState {
    object := &SyncState{
        hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*SyncState) ID() string {
    return StateSyncID
}

func (self *SyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    return nil
}

func (self *SyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // TODO add impl
    return nil
}

func (self *SyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    // TODO add impl
    return self.Super()
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

func NewMajorityCommitCondition(
    clusterSize uint32) *MajorityCommitCondition {

    return &MajorityCommitCondition{
        VoteCount:    0,
        ClusterSize:  clusterSize,
        MajoritySize: (clusterSize / 2) + 1,
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
    ToCommitEntries  []*InflightEntry
    CommittedEntries []*InflightEntry
    ClusterSize      uint32
    PeerMatchIndexes map[net.Addr]uint64

    sync.Mutex
}

func NewInflight(peers []net.Addr) *Inflight {
    peerMatchIndexes := make(map[net.Addr]uint64)
    for _, peer := range peers {
        peerMatchIndexes[peer] = 0
    }
    return &Inflight{
        MinIndex:         0,
        MaxIndex:         0,
        ToCommitEntries:  make([]*InflightEntry, 0),
        CommittedEntries: make([]*InflightEntry, 0),
        ClusterSize:      uint32(len(peers)),
        PeerMatchIndexes: peerMatchIndexes,
    }
}

func (self *Inflight) Init() {
    self.MinIndex = 0
    self.MaxIndex = 0
    self.ToCommitEntries = make([]*InflightEntry, 0)
    self.CommittedEntries = make([]*InflightEntry, 0)
    for k, _ := range self.PeerMatchIndexes {
        self.PeerMatchIndexes[k] = 0
    }
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
    for _, toCommit := range self.ToCommitEntries {
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
