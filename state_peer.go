package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "io"
    "sync"
    "sync/atomic"
    "time"
)

type PeerState struct {
    *LogStateHead
}

func NewPeerState(super hsm.State, logger logging.Logger) *PeerState {
    object := &PeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*PeerState) ID() string {
    return StatePeerID
}

func (self *PeerState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *PeerState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Init", self.ID())
    sm.QInit(StateDeactivatedPeerID)
    return nil
}

func (self *PeerState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *PeerState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    return self.Super()
}

type DeactivatedPeerState struct {
    *LogStateHead
}

func NewDeactivatedPeerState(
    super hsm.State, logger logging.Logger) *DeactivatedPeerState {

    object := &DeactivatedPeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*DeactivatedPeerState) ID() string {
    return StateDeactivatedPeerID
}

func (self *DeactivatedPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *DeactivatedPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *DeactivatedPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch event.Type() {
    case ev.EventPeerActivate:
        // TODO add log
        sm.QTran(StateActivatedPeerID)
        return nil
    }
    return self.Super()
}

type ActivatedPeerState struct {
    *LogStateHead
}

func NewActivatedPeerState(super hsm.State, logger logging.Logger) *ActivatedPeerState {
    object := &ActivatedPeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*ActivatedPeerState) ID() string {
    return StateActivatedPeerID
}

func (self *ActivatedPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *ActivatedPeerState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    sm.QInit(StateActivatedPeerID)
    return nil
}

func (self *ActivatedPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *ActivatedPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventRequestVoteRequest:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerAddr := peerHSM.Addr()
        response, err := peerHSM.Client().CallRPCTo(&peerAddr, e)
        if err != nil {
            // TODO add log
            return nil
        }
        peerHSM.SelfDispatch(response)
        return nil
    case ev.EventRequestVoteResponse:
        e, ok := event.(*ev.RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        e.FromAddr = peerHSM.Addr()
        peerHSM.EventHandler()(e)
        return nil
    case ev.EventPeerDeactivate:
        // TODO add log
        sm.QTran(StateDeactivatedPeerID)
        return nil
    }
    return self.Super()
}

type CandidatePeerState struct {
    *LogStateHead
}

func NewCandidatePeerState(super hsm.State, logger logging.Logger) *CandidatePeerState {
    object := &CandidatePeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*CandidatePeerState) ID() string {
    return StateCandidatePeerID
}

func (self *CandidatePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *CandidatePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *CandidatePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
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
    *LogStateHead

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
    lastContactTimeLock sync.RWMutex
}

func NewLeaderPeerState(
    super hsm.State,
    heartbeatTimeout time.Duration,
    logger logging.Logger) *LeaderPeerState {

    object := &LeaderPeerState{
        LogStateHead:     NewLogStateHead(super, logger),
        heartbeatTimeout: heartbeatTimeout,
        ticker:           NewRandomTicker(heartbeatTimeout),
    }
    super.AddChild(object)
    return object
}

func (*LeaderPeerState) ID() string {
    return StateLeaderPeerID
}

func (self *LeaderPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    // local initialization
    self.term = peerHSM.Local().GetCurrentTerm()
    self.matchIndex = 0
    lastLogIndex, err := peerHSM.Local().Log().LastIndex()
    if err != nil {
        // TODO error handling
    }
    self.nextIndex = lastLogIndex + 1
    self.UpdateLastContactTime()
    // trigger a check on whether to start log replication
    timeout := &ev.Timeout{
        LastTime: self.LastContactTime(),
        Timeout:  self.heartbeatTimeout,
    }
    peerHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
    // init timer
    deliverHearbeatTimeout := func() {
        lastContactTime := self.LastContactTime()
        if TimeExpire(lastContactTime, self.heartbeatTimeout) {
            timeout := &ev.Timeout{
                LastTime: lastContactTime,
                Timeout:  self.heartbeatTimeout,
            }
            peerHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
        }
    }
    self.ticker.Start(deliverHearbeatTimeout)
    return nil
}

func (self *LeaderPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // local cleanup
    self.ticker.Stop()
    return nil
}

func (self *LeaderPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    switch event.Type() {
    case ev.EventTimeoutHeartbeat:
        e, ok := event.(*ev.HeartbeatTimeoutEvent)
        hsm.AssertTrue(ok)
        notifyEvent := ev.NewNotifyHeartbeatTimeoutEvent(
            e.Message.LastTime, e.Message.Timeout)
        local.Notifier().Notify(notifyEvent)
        // check whether the peer falls behind the leader
        matchIndex, _ := self.GetIndexInfo()
        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }
        if matchIndex < lastLogIndex {
            sm.QTran(StateStandardModePeerID)
            return nil
        }
        // the peer is up-to-date, then send a pure heartbeat AE
        prevLogTerm, prevLogIndex, err := local.Log().LastEntryInfo()
        if err != nil {
            // TODO error handling
        }
        committedIndex, err := local.Log().CommittedIndex()
        if err != nil {
            // TODO error handling
        }
        request := &ev.AppendEntriesRequest{
            Term:              local.GetCurrentTerm(),
            Leader:            local.GetLocalAddr(),
            PrevLogTerm:       prevLogIndex,
            PrevLogIndex:      prevLogTerm,
            Entries:           make([]*ps.LogEntry, 0),
            LeaderCommitIndex: committedIndex,
        }
        requestEvent := ev.NewAppendEntriesRequestEvent(request)
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(&peerAddr, requestEvent)
        if err != nil {
            // TODO error handling
        }
        appendEntriesRespEvent, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            // TODO error handling
        }
        appendEntriesRespEvent.FromAddr = peerAddr
        // update last contact timer
        self.UpdateLastContact()
        peerHSM.SelfDispatch(appendEntriesRespEvent)
        return nil
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        if e.Response.Term > local.GetCurrentTerm() {
            local.SelfDispatch(ev.NewStepdownEvent())
        }
        matchIndex, _ := self.GetIndexInfo()
        if e.Response.LastLogIndex != matchIndex {
            // TODO add log
            if e.Response.LastLogIndex > matchIndex {
                message := &ev.PeerReplicateLog{
                    Peer:       peerHSM.Addr(),
                    MatchIndex: matchIndex,
                }
                event := ev.NewPeerReplicateLogEvent(message)
                local.SelfDispatch(event)
            }
            self.SetMatchIndex(e.Response.LastLogIndex)
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

func (self *LeaderPeerState) UpdateLastContact() {
    self.UpdateLastContactTime()
    self.ticker.Reset()
}

type StandardModePeerState struct {
    *LogStateHead

    maxAppendEntriesSize uint64
}

func NewStandardModePeerState(
    super hsm.State,
    maxAppendEntriesSize uint64,
    logger logging.Logger) *StandardModePeerState {

    object := &StandardModePeerState{
        LogStateHead:         NewLogStateHead(super, logger),
        maxAppendEntriesSize: maxAppendEntriesSize,
    }
    super.AddChild(object)
    return object
}

func (*StandardModePeerState) ID() string {
    return StateStandardModePeerID
}

func (self *StandardModePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    peerHSM.SelfDispatch(self.SetupReplicating(peerHSM))
    return nil
}

func (self *StandardModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit")
    return nil
}

func (self *StandardModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventTimeoutHeartbeat:
        event := self.SetupReplicating(peerHSM)
        peerHSM.SelfDispatch(event)
    case ev.EventAppendEntriesRequest:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(&peerAddr, e)
        if err != nil {
            // TODO error handling
        }
        appendEntriesRespEvent, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            // TODO error handling
        }
        appendEntriesRespEvent.FromAddr = peerAddr
        // update last contact timer
        leaderPeerState, ok := self.Super().(*LeaderPeerState)
        hsm.AssertTrue(ok)
        leaderPeerState.UpdateLastContact()
        // dispatch response to self, just jump to the next case block
        peerHSM.SelfDispatch(appendEntriesRespEvent)
        return nil
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        local := peerHSM.Local()
        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }
        if e.Response.LastLogIndex < lastLogIndex {
            // peer log has not caught up with us leader yet
            event := self.SetupReplicating(peerHSM)
            peerHSM.SelfDispatch(event)
        } else {
            // peer log has caught up with us leader already
            sm.QTran(StateLeaderPeerID)
        }
        return self.Super()
    case ev.EventPeerEnterSnapshotMode:
        // TODO add log
        sm.QTran(StateSnapshotModePeerID)
        return nil
    }
    return self.Super()
}

func (self *StandardModePeerState) SetupReplicating(
    peerHSM *PeerHSM) (event hsm.Event) {

    leaderPeerState, ok := self.Super().(*LeaderPeerState)
    hsm.AssertTrue(ok)
    matchIndex, nextIndex := leaderPeerState.GetIndexInfo()
    local := peerHSM.Local()
    _, lastSnapshotIndex, err := local.SnapshotManager().LastSnapshotInfo()
    if err != nil {
        // TODO error handling
    }
    switch {
    case matchIndex == 0:
        committedIndex, err := local.Log().CommittedIndex()
        if err != nil {
            // TODO add error handling
        }
        request := &ev.AppendEntriesRequest{
            Term:              local.GetCurrentTerm(),
            Leader:            local.GetLocalAddr(),
            PrevLogTerm:       0,
            PrevLogIndex:      0,
            Entries:           make([]*ps.LogEntry, 0),
            LeaderCommitIndex: committedIndex,
        }
        event = ev.NewAppendEntriesRequestEvent(request)
    case matchIndex < lastSnapshotIndex:
        event = ev.NewPeerEnterSnapshotModeEvent()
    default:
        log, err := local.Log().GetLog(matchIndex)
        if err != nil {
            // TODO error handling
        }
        prevLogIndex := log.Index
        prevLogTerm := log.Term

        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }
        entriesSize := Min(uint64(self.maxAppendEntriesSize),
            (lastLogIndex - matchIndex))
        maxIndex := nextIndex + entriesSize - 1
        logEntries := make([]*ps.LogEntry, 0, entriesSize)
        for i := nextIndex; i <= maxIndex; i++ {
            if log, err = local.Log().GetLog(i); err != nil {
                // TODO error handling
            }
            logEntries = append(logEntries, log)
        }
        committedIndex, err := local.Log().CommittedIndex()
        if err != nil {
            // TODO add error handling
        }
        request := &ev.AppendEntriesRequest{
            Term:              local.GetCurrentTerm(),
            Leader:            local.GetLocalAddr(),
            PrevLogTerm:       prevLogTerm,
            PrevLogIndex:      prevLogIndex,
            Entries:           logEntries,
            LeaderCommitIndex: committedIndex,
        }
        event = ev.NewAppendEntriesRequestEvent(request)
    }
    return event
}

type SnapshotModePeerState struct {
    *LogStateHead

    maxSnapshotChunkSize uint64
    offset               uint64
    lastChunk            []byte
    snapshotMeta         *ps.SnapshotMeta
    snapshotReadCloser   io.ReadCloser
}

func NewSnapshotModePeerState(
    super hsm.State,
    maxSnapshotChunkSize uint64,
    logger logging.Logger) *SnapshotModePeerState {

    object := &SnapshotModePeerState{
        LogStateHead:         NewLogStateHead(super, logger),
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

    self.Debug("STATE: %s, -> Entry")
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    snapshotMetas, err := local.SnapshotManager().List()
    if err != nil {
        // TODO error handling
    }
    if len(snapshotMetas) == 0 {
        // TODO error handling
    }

    id := snapshotMetas[0].ID
    meta, readCloser, err := local.SnapshotManager().Open(id)
    if err != nil {
        // TODO error handling
    }
    self.snapshotMeta = meta
    self.snapshotReadCloser = readCloser
    self.offset = 0
    self.lastChunk = make([]byte, 0)

    if err := self.SendNextChunk(
        peerHSM, local.GetCurrentTerm(), local.GetLocalAddr()); err != nil {

        peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
    }
    return nil
}

func (self *SnapshotModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
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

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    switch event.Type() {
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(&peerAddr, e)
        if err != nil {
            // TODO add log
        }
        if respEvent.Type() != ev.EventInstallSnapshotResponse {
            // TODO error handling
        }
        snapshotRespEvent, ok := respEvent.(*ev.InstallSnapshotResponseEvent)
        if !ok {
            // TODO error handling
        }

        // update last contact timer
        leaderPeerState, ok := self.Super().(*LeaderPeerState)
        hsm.AssertTrue(ok)
        leaderPeerState.UpdateLastContact()

        if snapshotRespEvent.Response.Term > local.GetCurrentTerm() {
            event := ev.NewStepdownEvent()
            local.SelfDispatch(event)
            // Stop replicating snapshot to this peer since it already
            // connected to another more up-to-date leader.
            peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
            return nil
        }

        if snapshotRespEvent.Response.Success {
            // last chunk replicated
            self.offset += uint64(len(self.lastChunk))
            if self.offset == self.snapshotMeta.Size {
                // all chunk send, snapshot replication done
                // TODO add log
                sm.QTran(StateStandardModePeerID)
                return nil
            }

            // send next chunk
            if err := self.SendNextChunk(
                peerHSM, local.GetCurrentTerm(), local.GetLocalAddr()); err != nil {

                // TODO add log
                peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
            }
        } else {
            // resend last chunk
            e := self.SetupRequest(local.GetCurrentTerm(), local.GetLocalAddr())
            peerHSM.SelfDispatch(e)
        }
        return nil
    case ev.EventPeerAbortSnapshotMode:
        // TODO add log
        sm.QTran(StateStandardModePeerID)
        return nil
    }
    return self.Super()
}

func (self *SnapshotModePeerState) SetupRequest(
    term uint64, leader ps.ServerAddr) hsm.Event {

    request := &ev.InstallSnapshotRequest{
        Term:              term,
        Leader:            leader,
        LastIncludedIndex: self.snapshotMeta.LastIncludedIndex,
        LastIncludedTerm:  self.snapshotMeta.LastIncludedTerm,
        Offset:            self.offset,
        Data:              self.lastChunk,
        Servers:           self.snapshotMeta.Servers,
        Size:              self.snapshotMeta.Size,
    }
    event := ev.NewInstallSnapshotRequestEvent(request)
    return event
}

func (self *SnapshotModePeerState) SendNextChunk(
    peerHSM *PeerHSM, term uint64, leader ps.ServerAddr) error {

    data := make([]byte, self.maxSnapshotChunkSize)
    n, err := self.snapshotReadCloser.Read(data)
    if n > 0 {
        self.lastChunk = data[:n]
        requestEvent := self.SetupRequest(term, leader)
        peerHSM.SelfDispatch(requestEvent)
        return nil
    } else {
        if err == io.EOF || err == nil {
            // TODO add log
            return errors.New("fail to read next chunk")
        } else {
            // TODO add log
            return err
        }
    }
}

type PipelineModePeerState struct {
    *LogStateHead
}

func NewPipelineModePeerState(
    super hsm.State, logger logging.Logger) *PipelineModePeerState {

    object := &PipelineModePeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*PipelineModePeerState) ID() string {
    return StatePipelineModePeerID
}

func (self *PipelineModePeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *PipelineModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *PipelineModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    // TODO add impl
    return self.Super()
}
