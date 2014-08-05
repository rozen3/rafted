package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "sync"
    "time"
)

type PeerState struct {
    *hsm.StateHead
}

func NewPeerState(super hsm.State) *PeerState {
    object := &PeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerState) ID() string {
    return StatePeerID
}

func (self *PeerState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerDeactivatedID)
    return nil
}

func (self *PeerState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event =", event)
    return self.Super()
}

type PeerDeactivatedState struct {
    *hsm.StateHead
}

func NewPeerDeactivateState(super hsm.State) *PeerDeactivatedState {
    object := &PeerDeactivatedState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerDeactivatedState) ID() string {
    return StatePeerDeactivatedID
}

func (self *PeerDeactivatedState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerDeactivatedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerDeactivatedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    switch event.Type() {
    case ev.EventPeerActivate:
        // TODO add log
        sm.QTranOnEvent(StatePeerActivatedID, event)
        return nil
    }
    return self.Super()
}

type PeerActivatedState struct {
    *hsm.StateHead

    // term of highest log entry known to be replicated on the peer
    term uint64
    // index of highest log entry known to be replicated on the peer
    matchIndex uint64
    // index of the next log entry to send to that peer
    nextIndex uint64
    // lock for matchIndex and nextIndex
    indexLock sync.RWMutex
    // heartbeat timeout and its time ticker
    heartbeatTimeout time.Duration
    ticker           Ticker
    // last time we have contact from the peer
    lastContactTime     time.Time
    LastContactTimeLock sync.RWMutex

    // configuration
    maxAppendEntriesSize uint64
}

func NewPeerActivatedState(
    super hsm.State,
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64) *PeerActivatedState {

    object := &PeerActivatedState{
        StateHead:            hsm.NewStateHead(super),
        heartbeatTimeout:     heartbeatTimeout,
        ticker:               NewRandomTicker(heartbeatTimeout),
        maxAppendEntriesSize: maxAppendEntriesSize,
    }
    super.AddChild(object)
    return object
}

func (*PeerActivatedState) ID() string {
    return StatePeerActivatedID
}

func (self *PeerActivatedState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    activateEvent, ok := event.(*ev.PeerActivateEvent)
    hsm.AssertTrue(ok)
    // local initialization
    self.term = activateEvent.Message.Term
    self.matchIndex = 0
    self.nextIndex = activateEvent.Message.LastLogIndex + 1
    // init timer
    self.UpdateLastContactTime()
    deliverHearbeatTimeout := func() {
        lastContactTime := self.LastContactTime()
        if TimeExpire(lastContactTime, self.heartbeatTimeout) {
            timeout := &HeartbeatTimeout{
                LastContactTime: lastContactTime,
                Timeout:         self.heartbeatTimeout,
            }
            peerHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
            self.UpdateLastContactTime()
        }
    }
    self.ticker.Start(deliverHearbeatTimeout)
    return nil
}

func (self *PeerActivatedState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerIdleID)
    return nil
}

func (self *PeerActivatedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // local cleanup
    self.ticker.Stop()
    return nil
}

func (self *PeerActivatedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    switch event.Type() {
    case ev.EventPeerDeactivate:
        // TODO add log
        sm.QTran(StatePeerDeactivatedID)
        return nil
    }
    return self.Super()
}

func (self *PeerActivatedState) GetTerm() uint64 {
    return atomic.LoadUint64(&self.term)
}

func (self *PeerActivatedState) SetTerm(term uint64) {
    atomic.StoreUint64(&self.term, term)
}

func (self *PeerActivatedState) GetIndexInfo() (uint64, uint64) {
    self.indexLock.RLock()
    defer self.indexLock.RUnlock()
    return self.matchIndex, self.nextIndex
}

func (self *PeerActivatedState) SetMatchIndex(index uint64) {
    self.indexLock.Lock()
    defer self.indexLock.Unlock()
    self.matchIndex = index
    self.nextIndex = self.matchIndex + 1
}

func (self *PeerActivatedState) LastContactTime() time.Time {
    self.lastContactTimeLock.RLock()
    defer self.lastContactTimeLock.RUnlock()
    return self.lastContactTime
}

func (self *PeerActivatedState) UpdateLastContactTime() {
    self.lastContactTimeLock.Lock()
    defer self.lastContactTimeLock.Unlock()
    self.lastContactTime = time.Now()
}

func (self *PeerActivatedState) GetMaxAppendEntriesSize() uint64 {
    return atomic.LoadUint64(&self.maxAppendEntriesSize)
}

func (self *PeerActivatedState) SetMaxAppendEntriesSize(size uint64) {
    atomic.StoreUint64(&self.maxAppendEntriesSize, size)
}

type PeerIdleState struct {
    *hsm.StateHead
}

func NewPeerIdleState(super hsm.State) *PeerIdleState {
    object := &PeerIdleState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerIdleState) ID() string {
    return StatePeerIdleID
}

func (self *PeerIdleState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerIdleState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    peerHSM.SelfDispatch(ev.NewPeerCheckLogReplicationEvent())
    return self.Super()
}

func (self *PeerIdleState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerIdleState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch {
    case ev.IsRaftRequest(event.Type()):
        peerHSM.SelfDispatch(event)
        sm.QTran(StatePeerReplicatingID)
        return nil
    case ev.EventHeartbeatTimeout:
        fallthrough
    case ev.EventPeerCheckLogReplication:
        // check whether the peer falls behind the leader
        activatedState, ok := self.Super().(*PeerActivatedState)
        hsm.AssertTrue(ok)
        matchIndex, nextIndex := activatedState.GetIndexInfo()
        if matchIndex < peerHSM.GetLog().LastIndex() {
            sm.QTran(StatePeerReplicatingID)
        }
        return nil
    }
    return self.Super()
}

type PeerReplicatingState struct {
    *hsm.StateHead
}

func NewPeerReplicatingState(super hsm.State) *PeerReplicatingState {
    object := &PeerReplicatingState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerReplicatingState) ID() string {
    return StatePeerReplicatingID
}

func (self *PeerReplicatingState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    return nil
}

func (self *PeerReplicatingState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerStandardModeID)
    return nil
}

func (self *PeerReplicatingState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerReplicatingState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    return self.Super()
}

type PeerStandardModeState struct {
    *hsm.StateHead
}

func NewPeerStandardModeState(
    super hsm.State,
    maxSnapshotChunkSize uint64) *PeerStandardModeState {

    object := &PeerStandardModeState{
        StateHead:            hsm.NewStateHead(super),
        maxSnapshotChunkSize: maxSnapshotChunkSize,
    }
    super.AddChild(object)
    return object
}

func (*PeerStandardModeState) ID() string {
    return StatePeerStandardModeID
}

func (self *PeerStandardModeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")

    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    activatedState, ok := self.Super().Super().(*PeerActivatedState)
    hsm.AssertTrue(ok)

    request := MakeAppendEntriesRequest(peerHSM, activatedState)
    peerHSM.SelfDispatch(request)
    return nil
}

func (self *PeerStandardModeState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    return self.Super()
}

func (self *PeerStandardModeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerStandardModeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch {
    case ev.IsRaftRequest(event.Type()):
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        response, err := peerHSM.Client.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO add log
            return nil
        }
        peerHSM.SelfDispatch(response)
        return nil
    case event.Type() == ev.EventRequestVoteResponse:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerHSM.EventHandler(e)
        return nil
    case event.Type() == ev.EventAppendEntriesResponse:
        // TODO
        return nil
    case event.Type() == ev.EventInstallSnapshotResponse:
        // TODO
        return nil
    }
    // ignore all other events
    return self.Super()
}

func (self *PeerStandardModeState) SetupReplicatingRequestEvent(
    peerHSM *PeerHSM,
    activatedState *PeerActivatedState) hsm.Event {

    matchIndex, nextIndex := activatedState.GetIndexInfo()
    raftHSM := peerHSM.GetRaftHSM()
    lastSnapshotIndex := raftHSM.GetLastSnapshotIndex()
    switch {
    case nextIndex == 1:
        leader, err := EncodeAddr(raftHSM.LocalAddr)
        if err != nil {
            // TODO add error handling
        }

        request := &ev.AppendEntriesRequest{
            Term:              raftHSM.GetCurrentTerm(),
            Leader:            leader,
            PrevLogTerm:       0,
            PrevLogIndex:      0,
            Entries:           make([]*persist.LogEntry, 0),
            LeaderCommitIndex: raftHSM.GetCommitIndex(),
        }
        event := ev.NewAppendEntriesRequestEvent(request)
    case matchIndex < lastSnapshotIndex:
        event := ev.NewPeerEnterSnapshotModeEvent
        snapshotManager := raftHSM.GetSnapshotManager()
        snapshotMetas, err := snapshotManager.list()
        if err != nil {
            // TODO error handling
        }
        if len(snapshots) == 0 {
            // TODO error handling
        }

        id := snapshotMetas[0].ID
        meta, snapshot, err := snapshotManager.Open(id)
        if err != nil {
            // TODO error handling
        }
        defer snapshot.Close()
        // TODO
    case matchIndex == lastSnapshotIndex:
        log, err := peerHSM.GetRaftHSM().GetLog().GetLog(nextIndex)
        if err != nil {
            // TODO add log
        }
    default:
        // TODO
    }

}

type PeerSnapshotModeState struct {
    *hsm.StateHead

    maxSnapshotChunkSize uint64
    snapshotMeta         *persist.SnapshotMeta
    snapshotReadCloser   io.ReadCloser
}

func NewPeerSnapshotModeState(
    super hsm.State,
    maxSnapshotChunkSize uint64) *PeerSnapshotModeState {

    object := &PeerSnapshotModeState{
        maxSnapshotChunkSize: maxSnapshotChunkSize,
    }
    super.AddChild(object)
    return object
}

func (*PeerSnapshotModeState) ID() string {
    return StatePeerSnapshotModeID
}

func (self *PeerSnapshotModeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerSnapshotModeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
}

func (self *PeerSnapshotModeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    return self.Super()
}

type PeerPipelineModeState struct {
    *hsm.StateHead
}

func NewPeerPipelineModeState(super hsm.State) *PeerPipelineModeState {
    object := &PeerPipelineModeState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerPipelineModeState) ID() string {
    return StatePeerPipelineModeID
}

func (self *PeerPipelineModeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerPipelineModeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerPipelineModeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    // TODO add impl
    return self.Super()
}
