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

type DeactivatePeerState struct {
    *hsm.StateHead
}

func NewPeerDeactivateState(super hsm.State) *DeactivatePeerState {
    object := &DeactivatePeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*DeactivatePeerState) ID() string {
    return StateDeactivatePeerID
}

func (self *DeactivatePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *DeactivatePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *DeactivatePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    switch event.Type() {
    case ev.EventPeerActivate:
        // TODO add log
        sm.QTran(StateActivatedPeerID)
        return nil
    }
    return self.Super()
}

type ActivatedPeerState struct {
    *hsm.StateHead
}

func NewActivatedPeerState(
    super hsm.State) *ActivatedPeerState {

    object := &ActivatedPeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*ActivatedPeerState) ID() string {
    return StateActivatedPeerID
}

func (self *ActivatedPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *ActivatedPeerState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerIdleID)
    return nil
}

func (self *ActivatedPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *ActivatedPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    switch event.Type() {
    case ev.EventRequestVoteRequest:
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
    case ev.EventPeerDeactivate:
        // TODO add log
        sm.QTran(StateDeactivatePeerID)
        return nil
    }
    return self.Super()
}

type CandidatePeerState struct {
    *hsm.StateHead
}

func NewCandidatePeerState(super hsm.State) *CandidatePeerState {
    object := &CandidatePeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*CandidatePeerState) ID() string {
    return StateCandidatePeerID
}

func (self *CandidatePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *CandidatePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *CandidatePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    switch event.Type() {
    case ev.EventAppendEntriesRequest:
        peerHSM, ok := sm.(*PeerHSM)
        hsm.AssertTrue(ok)
        peerHSM.SelfDispatch(event)
        sm.QTran(StateLeaderPeerID)
        return nil
    case ev.EventPeerEnterLeader:
        sm.QTran(StateLeaderPeerID)
        return nil
    }
    return self.Super()
}

type LeaderPeerState struct {
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

func NewLeaderPeerState(
    super hsm.State,
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64) *LeaderPeerState {

    object := &LeaderPeerState{
        StateHead:            hsm.NewStateHead(super),
        heartbeatTimeout:     heartbeatTimeout,
        ticker:               NewRandomTicker(heartbeatTimeout),
        maxAppendEntriesSize: maxAppendEntriesSize,
    }
    super.AddChild(object)
    return object
}

func (*LeaderPeerState) ID() string {
    return StateLeaderPeerID
}

func (self *LeaderPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    // local initialization
    self.term = peerHSM.GetRaftHSM().GetCurrentTerm()
    self.matchIndex = 0
    self.nextIndex = peerHSM.GetRaftHSM().GetLog().LastIndex() + 1
    // trigger a check on whether to start log replication
    peerHSM.SelfDispatch(ev.NewPeerCheckLogReplicationEvent())
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
        }
    }
    self.ticker.Start(deliverHearbeatTimeout)
    return nil
}

func (self *LeaderPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // local cleanup
    self.ticker.Stop()
    return nil
}

func (self *LeaderPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch {
    case ev.EventHeartbeatTimeout:
        fallthrough
    case ev.EventPeerCheckLogReplication:
        // check whether the peer falls behind the leader
        matchIndex, nextIndex := self.GetIndexInfo()
        if matchIndex < peerHSM.GetLog().LastIndex() {
            sm.QTran(StatePeerStandardModeID)
            return nil
        }
        // the peer is up-to-date, then send a pure heartbeat AE
        raftHSM := peerHSM.GetRaftHSM()
        leader, err := EncodeAddr(raftHSM.LocalAddr)
        if err != nil {
            // TODO error handling
        }
        prevLogIndex := raftHSM.GetLog().LastIndex()
        log, err := raftHSM.GetLog().GetLog(prevLogIndex)
        if err != nil {
            // TODO error handling
        }
        prevLogTerm := log.Term
        request := &ev.AppendEntriesRequest{
            Term:              raftHSM.GetCurrentTerm(),
            Leader:            leader,
            PrevLogTerm:       prevLogIndex,
            PrevLogIndex:      prevLogTerm,
            Entries:           make([]*persist.LogEntry, 0),
            LeaderCommitIndex: raftHSM.GetCommitIndex(),
        }
        event := ev.NewAppendEntriesRequestEvent(request)
        respEvent, err := peerHSM.Client.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO error handling
        }
        e, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            // TODO error handling
        }
        peerHSM.SelfDispatch(e)
        return nil
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        raftHSM := peerHSM.GetRaftHSM()
        if e.Response.Term > raftHSM.GetCurrentTerm() {
            raftHSM.SelfDispatch(ev.NewStepdownEvent())
        }
        if e.Response.LastLogIndex != self.GetMatchIndex() {
            // TODO add log
            if e.Reponse.LastLogIndex > self.GetMatchIndex() {
                message := &ev.PeerReplicateLog{
                    Peer:       peerHSM.Addr,
                    MatchIndex: self.GetMatchIndex(),
                }
                event := ev.NewPeerReplicateLogEvent(message)
                raftHSM.SelfDispatch(event)
            }
            self.SetMatchIndex(e.Response.LastLogIndex)
        }
        // don't care whether response is success
        self.UpdateLastContactTime()
        // check whether the peer is up-to-date
        matchIndex, nextIndex := self.GetIndexInfo()
        if matchIndex == peerHSM.GetLog().LastIndex() {
            sm.QTran(StateLeaderPeerID)
        }
        return nil
    }
    return self.Super()
}

func (self *LeaderPeerState) GetTerm() uint64 {
    return atomic.LoadUint64(&self.term)
}

func (self *LeaderPeerState) SetTerm(term uint64) {
    atomic.StoreUint64(&self.term, term)
}

func (self *LeaderPeerState) GetIndexInfo() (uint64, uint64) {
    self.indexLock.RLock()
    defer self.indexLock.RUnlock()
    return self.matchIndex, self.nextIndex
}

func (self *LeaderPeerState) SetMatchIndex(index uint64) {
    self.indexLock.Lock()
    defer self.indexLock.Unlock()
    self.matchIndex = index
    self.nextIndex = self.matchIndex + 1
}

func (self *LeaderPeerState) LastContactTime() time.Time {
    self.lastContactTimeLock.RLock()
    defer self.lastContactTimeLock.RUnlock()
    return self.lastContactTime
}

func (self *LeaderPeerState) UpdateLastContactTime() {
    self.lastContactTimeLock.Lock()
    defer self.lastContactTimeLock.Unlock()
    self.lastContactTime = time.Now()
}

func (self *LeaderPeerState) GetMaxAppendEntriesSize() uint64 {
    return atomic.LoadUint64(&self.maxAppendEntriesSize)
}

func (self *LeaderPeerState) SetMaxAppendEntriesSize(size uint64) {
    atomic.StoreUint64(&self.maxAppendEntriesSize, size)
}

type StandardModePeerState struct {
    *hsm.StateHead
}

func NewStandardModePeerState(
    super hsm.State,
    maxSnapshotChunkSize uint64) *StandardModePeerState {

    object := &StandardModePeerState{
        StateHead:            hsm.NewStateHead(super),
        maxSnapshotChunkSize: maxSnapshotChunkSize,
    }
    super.AddChild(object)
    return object
}

func (*StandardModePeerState) ID() string {
    return StateStandardModePeerID
}

func (self *StandardModePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")

    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    replicatingState, ok := self.Super().(*LeaderPeerState)
    hsm.AssertTrue(ok)
    event := self.SetupReplicating(peerHSM, replicatingState)
    peerHSM.SelfDispatch(event)
    return nil
}

func (self *StandardModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *StandardModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventHeartbeatTimeout:
        event := self.SetupReplicating(peerHSM)
        peerHSM.SelfDispatch(event)
    case ev.EventAppendEntriesRequest:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        respEvent, err := peerHSM.CLient.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO error handling
        }
        e, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            // TODO error handling
        }
        peerHSM.SelfDispatch(e)
        return nil
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        raftHSM := peerHSM.GetRaftHSM()
        if e.Response.LastLogIndex < raftHSM.GetLog().LastIndex() {
            // not done replicating the peer log
            event := self.SetupReplicating(peerHSM)
            peerHSM.SelfDispatch(event)
        }
        return self.Super()
    case ev.EventEnterSnapshotMode:
        // TODO add log
        sm.QTran(StateSnapshotModePeerID)
        return nil
    }
    return self.Super()
}

func (self *StandardModePeerState) SetupReplicating(
    peerHSM *PeerHSM,
    activatedState *ActivatedPeerState) (event hsm.Event) {

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
        event = ev.NewAppendEntriesRequestEvent(request)
    case matchIndex < lastSnapshotIndex:
        event = ev.NewPeerEnterSnapshotModeEvent()
    default:
        log, err := raftHSM.GetLog().GetLog(nextIndex)
        if err != nil {
            // TODO error handling
        }
        prevLogIndex := log.Index
        prevLogTerm := log.Term

        logEntries := make([]*persist.LogEntry, 0, self.maxAppendEntriesSize)
        maxIndex := Min(
            nextIndex+uint64(self.maxAppendEntriesSize)-1,
            raftHSM.GetLog().LastIndex())
        for i := nextIndex; i <= maxIndex; i++ {
            if log, err = raftHSM.GetLog().GetLog(i); err != nil {
                // TODO error handling
            }
            logEntries = append(logEntries, log)
        }
        request := &ev.AppendEntriesRequest{
            Term:              raftHSM.GetCurrentTerm(),
            Leader:            leader,
            PrevLogTerm:       prevLogTerm,
            PrevLogIndex:      prevLogIndex,
            Entries:           logEntries,
            LeaderCommitIndex: raftHSM.GetCommitIndex(),
        }
        event = ev.NewAppendEntriesRequestEvent(request)
    }
    return event
}

type SnapshotModePeerState struct {
    *hsm.StateHead

    maxSnapshotChunkSize uint64
    snapshotOffset       uint64
    lastChunk            []byte
    snapshotMeta         *persist.SnapshotMeta
    snapshotReadCloser   io.ReadCloser
}

func NewSnapshotModePeerState(
    super hsm.State,
    maxSnapshotChunkSize uint64) *SnapshotModePeerState {

    object := &SnapshotModePeerState{
        maxSnapshotChunkSize: maxSnapshotChunkSize,
    }
    super.AddChild(object)
    return object
}

func (*SnapshotModePeerState) ID() string {
    return StateSnapshotModePeerID
}

func (self *SnapshotModePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")

    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    raftHSM := peerHSM.GetRaftHSM()
    snapshotManager := raftHSM.GetSnapshotManager()
    snapshotMetas, err := snapshotManager.list()
    if err != nil {
        // TODO error handling
    }
    if len(snapshots) == 0 {
        // TODO error handling
    }

    id := snapshotMetas[0].ID
    meta, readCloser, err := snapshotManager.Open(id)
    if err != nil {
        // TODO error handling
    }
    self.snapshotMeta = meta
    self.snapshotReadCloser = readCloser

    // TODO
    leader, err := EncodeAddr(raftHSM.LocalAddr)
    if err != nil {
        // err handling
    }

    self.offset = 0
    self.lastChunk = make([]byte, 0)
    data := make([]byte, self.maxSnapshotChunkSize)
    n, err := self.snapshotReadCloser.Read(data)
    if n > 0 {
        self.lastChunk = data[:n]
        request := &ev.InstallSnapshotRequest{
            Term:              raftHSM.GetCurrentTerm(),
            Leader:            leader,
            LastIncludedIndex: self.snapshotMeta.LastIncludedIndex,
            LastIncludedTerm:  self.snapshotMeta.LastIncludedTerm,
            Offset:            self.offset,
            Data:              self.lastChunk,
            // TODO add servers
            Servers: make([]byte, 0),
            Size:    self.snapshotMeta.Size,
        }
        event := ev.NewInstallSnapshotRequestEvent(request)
    } else {
        if err == io.EOF || err == nil {
            // TODO add log
        } else {
            // TODO add log
        }
        event := ev.NewPeerAbortSnapshotModeEvent()
    }
    peerHSM.SelfDispatch(event)
    return nil
}

func (self *SnapshotModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    if err := self.snapshotReadCloser.Close(); err != nil {
        // TODO error handling
    }
    self.snapshotMeta = nil
    self.offset = 0
    self.lastChunk = nil
    return nil
}

func (self *SnapshotModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    raftHSM := peerHSM.GetRaftHSM()
    switch event.Type() {
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        respEvent, err := peerHSM.Client.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO add log
            nil
        }
        if responseEvent.Type() != ev.EventInstallSnapshotResponse {
            // TODO error handling
        }
        snapshotRespEvent, ok := respEvent.(*ev.InstallSnapshotResponseEvent)
        if !ok {
            // TODO error handling
        }
        if snapshotRespEvent.Response.Term > raftHSM.GetCurrentTerm() {
            event := ev.NewStepdownEvent()
            raftHSM.SelfDispatch(event)
            peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
            return nil
        }
        if snapshotRespEvent.Response.Success {
            // send next chunk
            self.offset += len(self.lastChunk)
            if self.offset == self.snapshotMeta.Size {
                // all snapshot send
                // TODO add log
                sm.QTran(StateStandardModePeerID)
                return nil
            }

            data := make([]byte, self.maxSnapshotChunkSize)
            n, err := self.snapshotReadCloser.Read(data)
            if n > 0 {
                self.lastChunk = data[:n]
                request := &ev.InstallSnapshotRequest{
                    Term:              raftHSM.GetCurrentTerm(),
                    Leader:            leader,
                    LastIncludedIndex: self.snapshotMeta.LastIncludedIndex,
                    LastIncludedTerm:  self.snapshotMeta.LastIncludedTerm,
                    Offset:            self.offset,
                    Data:              self.lastChunk,
                    // TODO add servers
                    Servers: make([]byte, 0),
                    Size:    self.snapshotMeta.Size,
                }
                event := ev.NewInstallSnapshotRequestEvent(request)
                peerHSM.SelfDispatch(event)
            } else {
                if err == io.EOF || err == nil {
                    // TODO error handling
                } else {
                    // TODO error handling
                }
            }
        } else {
            // resend last chunk
            request := &ev.InstallSnapshotRequest{
                Term:              raftHSM.GetCurrentTerm(),
                Leader:            leader,
                LastIncludedIndex: self.snapshotMeta.LastIncludedIndex,
                LastIncludedTerm:  self.snapshotMeta.LastIncludedTerm,
                Offset:            self.offset,
                Data:              self.lastChunk,
                // TODO add servers
                Servers: make([]byte, 0),
                Size:    self.snapshotMeta.Size,
            }
            event := ev.NewInstallSnapshotRequestEvent(request)
            peerHSM.SelfDispatch(event)
        }
        return nil
    case ev.EventPeerAbortSnapshotMode:
        // TODO add log
        sm.QTran(StateStandardModePeerID)
        return nil
    }
    return self.Super()
}

type PipelineModePeerState struct {
    *hsm.StateHead
}

func NewPipelineModePeerState(super hsm.State) *PipelineModePeerState {
    object := &PipelineModePeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PipelineModePeerState) ID() string {
    return StatePipelineModePeerID
}

func (self *PipelineModePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PipelineModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PipelineModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    // TODO add impl
    return self.Super()
}
