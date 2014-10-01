package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "io"
    "strings"
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
        ev.EventString(event))
    switch event.Type() {
    case ev.EventQueryStateRequest:
        peerHSM, ok := sm.(*PeerHSM)
        hsm.AssertTrue(ok)
        e, ok := event.(*ev.QueryStateRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.QueryStateResponse{
            StateID: peerHSM.StdHSM.State.ID(),
        }
        e.SendResponse(ev.NewQueryStateResponseEvent(response))
        return nil
    case ev.EventPersistError:
        sm.QTran(StatePersistErrorPeerID)
        return nil
    case ev.EventTerm:
        sm.QTran(hsm.TerminalStateID)
        return nil
    }
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
    return self.Super()
}

func (self *DeactivatedPeerState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    return self.Super()
}

func (self *DeactivatedPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *DeactivatedPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    switch event.Type() {
    case ev.EventPeerActivate:
        self.Debug("about to activate peer")
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
    sm.QInit(StateCandidatePeerID)
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
        ev.EventString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        peerAddr := peerHSM.Addr()
        self.Debug("peer to send RequestVoteRequest %#v to %s",
            e.Request, peerAddr.String())
        respEvent, err := peerHSM.Client().CallRPCTo(peerAddr, e)
        if err != nil {
            self.Error(
                "fail to call rpc RequestVoteRequest to peer: %s, error: %s",
                peerAddr.String(), err)
            return nil
        }
        requestVoteResponseEvent, ok := respEvent.(*ev.RequestVoteResponseEvent)
        if !ok {
            self.Error("receive non RequestVoteResponse for RequestVoteRequest")
            return nil
        }
        requestVoteResponseEvent.FromAddr = peerHSM.Addr()
        self.Debug("peer receive RequestVoteResponse %#v from %s",
            requestVoteResponseEvent.Response, peerAddr.String())
        peerHSM.SelfDispatch(respEvent)
        return nil
    case ev.EventRequestVoteResponse:
        e, ok := event.(*ev.RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        self.Debug("peer to dispatch RequestVoteResponse %#v from %s to local",
            e.Response, e.FromAddr.String())
        peerHSM.EventHandler()(e)
        return nil
    case ev.EventPeerDeactivate:
        self.Debug("about to deactivate peer")
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

func (self *CandidatePeerState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    return self.Super()
}

func (self *CandidatePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *CandidatePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
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
    // whether the matchIndex is synced from peer
    matchIndexUpdated bool
    indexUpdatedLock  sync.RWMutex
    // heartbeat timeout and its time ticker
    heartbeatTimeout time.Duration
    maxTimeoutJitter float32
    ticker           Ticker
    // last time we have contact from the peer
    lastContactTime     time.Time
    lastContactTimeLock sync.RWMutex
}

func NewLeaderPeerState(
    super hsm.State,
    heartbeatTimeout time.Duration,
    maxTimeoutJitter float32,
    logger logging.Logger) *LeaderPeerState {

    object := &LeaderPeerState{
        LogStateHead:     NewLogStateHead(super, logger),
        heartbeatTimeout: heartbeatTimeout,
        maxTimeoutJitter: maxTimeoutJitter,
        ticker:           NewRandomTicker(heartbeatTimeout, maxTimeoutJitter),
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
    local := peerHSM.Local()
    self.term = local.GetCurrentTerm()
    self.SetMatchIndex(0)
    self.SetMatchIndexUpdated(false)
    self.UpdateLastContactTime()
    // trigger a check on whether to start log replication
    timeout := &ev.Timeout{
        LastTime: self.LastContactTime(),
        Timeout:  self.heartbeatTimeout,
    }
    peerHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
    // init timer
    onTimeout := func() {
        timeout := &ev.Timeout{
            LastTime: self.LastContactTime(),
            Timeout:  self.heartbeatTimeout,
        }
        peerHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
    }
    self.ticker.Start(onTimeout)
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
        ev.EventString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    switch event.Type() {
    case ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        hsm.AssertTrue(ok)
        matchIndex, _ := self.GetIndexInfo()
        if e.Request.PrevLogIndex < matchIndex {
            self.Debug("ignore stale AppendEntriesRequest %#v, "+
                "PrevLogIndex: %d < matchIndex: %d",
                e.Request, e.Request.PrevLogIndex, matchIndex)
            return nil
        }
        self.Debug("call AE RPC to peer with Term: %d, PrevLogTerm: %d, "+
            "PrevLogIndex: %d, Entries size: %d, LeaderCommitIndex: %d, "+
            "entries info: %s",
            e.Request.Term, e.Request.PrevLogTerm, e.Request.PrevLogIndex,
            len(e.Request.Entries), e.Request.LeaderCommitIndex,
            "["+strings.Join(EntriesInfo(e.Request.Entries), ",")+"]")
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(peerAddr, e)
        if err != nil {
            self.Error(
                "fail to call rpc AppendEntriesRequest to peer: %s, error: %s",
                peerAddr.String(), err)
            return nil
        }
        appendEntriesRespEvent, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            self.Error("receive non AppendEntriesResponse for AppendEntriesRequest")
            return nil
        }
        appendEntriesRespEvent.FromAddr = peerAddr
        // update last contact timer
        self.UpdateLastContact()
        peerHSM.SelfDispatch(appendEntriesRespEvent)
        return nil
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        self.HandleAppendEntriesResponse(local, peerHSM, e.Response)
        return nil
    case ev.EventTimeoutHeartbeat:
        e, ok := event.(*ev.HeartbeatTimeoutEvent)
        hsm.AssertTrue(ok)
        local.Notifier().Notify(ev.NewNotifyHeartbeatTimeoutEvent(
            e.Message.LastTime, e.Message.Timeout))
        // check whether the peer falls behind the leader
        matchIndex, _ := self.GetIndexInfo()
        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            local.SendPrior(ev.NewPersistErrorEvent(errors.New(
                "fail to read last log index of log")))
            return nil
        }
        if matchIndex < lastLogIndex {
            sm.QTran(StateStandardModePeerID)
            return nil
        }
        // the peer is up-to-date, then send a pure heartbeat AE
        prevLogTerm, prevLogIndex, err := local.Log().LastEntryInfo()
        if err != nil {
            local.SendPrior(ev.NewPersistErrorEvent(
                errors.New("fail to read last entry info of log")))
            return nil
        }
        committedIndex, err := local.Log().CommittedIndex()
        if err != nil {
            local.SendPrior(ev.NewPersistErrorEvent(
                errors.New("fail to read committed index of log")))
            return nil
        }
        request := &ev.AppendEntriesRequest{
            Term:              local.GetCurrentTerm(),
            Leader:            local.GetLocalAddr(),
            PrevLogTerm:       prevLogTerm,
            PrevLogIndex:      prevLogIndex,
            Entries:           make([]*ps.LogEntry, 0),
            LeaderCommitIndex: committedIndex,
        }
        self.Debug("LeaderPeer on HeartbeatTimeout matchIndex == %d, AE, "+
            "Term: %d, PrevLogTerm: %d, PrevLogIndex: %d, Entries size: %d, "+
            "LeaderCommitIndex: %d, entries info: %s",
            matchIndex, request.Term, request.PrevLogTerm, request.PrevLogIndex,
            len(request.Entries), request.LeaderCommitIndex,
            "["+strings.Join(EntriesInfo(request.Entries), ",")+"]")
        requestEvent := ev.NewAppendEntriesRequestEvent(request)
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(peerAddr, requestEvent)
        if err != nil {
            self.Error(
                "fail to call rpc AppendEntriesRequest to peer: %s, error: %s",
                peerAddr.String(), err)
            return nil
        }
        appendEntriesRespEvent, ok := respEvent.(*ev.AppendEntriesResponseEvent)
        if !ok {
            self.Error("receive non AppendEntriesResponse for AppendEntriesRequest")
            return nil
        }
        appendEntriesRespEvent.FromAddr = peerAddr
        // update last contact timer
        self.UpdateLastContact()
        peerHSM.SelfDispatch(appendEntriesRespEvent)
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

func (self *LeaderPeerState) GetMatchIndexUpdated() bool {
    self.indexUpdatedLock.RLock()
    defer self.indexUpdatedLock.RUnlock()
    return self.matchIndexUpdated
}

func (self *LeaderPeerState) SetMatchIndexUpdated(v bool) {
    self.indexUpdatedLock.Lock()
    defer self.indexUpdatedLock.Unlock()
    self.matchIndexUpdated = v
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

func (self *LeaderPeerState) HandleAppendEntriesResponse(local Local, peerHSM *PeerHSM, response *ev.AppendEntriesResponse) {
    term := local.GetCurrentTerm()
    if response.Term > term {
        self.Debug("receive AppendEntriesResponse with newer term: %d, "+
            "local term: %d, about to stepdown", response.Term, term)
        local.SendPrior(ev.NewStepdownEvent())
    }
    matchIndex, _ := self.GetIndexInfo()
    peerAddr := peerHSM.Addr()
    if response.LastLogIndex > matchIndex {
        self.Debug("the LastLogIndex of peer %s updates from %d to %d",
            peerAddr.String(), matchIndex, response.LastLogIndex)
        message := &ev.PeerReplicateLog{
            Peer:       peerHSM.Addr(),
            MatchIndex: response.LastLogIndex,
        }
        event := ev.NewPeerReplicateLogEvent(message)
        local.SendPrior(event)
    } else if response.LastLogIndex == matchIndex {
        self.Debug("the LastLogIndex of peer %s stays %d on AE",
            peerAddr.String(), response.LastLogIndex)
    } else {
        self.Error("the LastLogIndex of peer %s backwards from %d to %d",
            peerAddr.String(), matchIndex, response.LastLogIndex)
    }
    self.SetMatchIndex(response.LastLogIndex)
    self.SetMatchIndexUpdated(true)
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

    self.Debug("STATE: %s, -> Entry", self.ID())
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    peerHSM.SelfDispatch(self.SetupReplicating(peerHSM))
    return nil
}

func (self *StandardModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *StandardModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        local := peerHSM.Local()
        leaderPeerState, ok := self.Super().(*LeaderPeerState)
        hsm.AssertTrue(ok)
        leaderPeerState.HandleAppendEntriesResponse(local, peerHSM, e.Response)
        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            local.SendPrior(ev.NewPersistErrorEvent(errors.New(
                "fail to read last log index of log")))
            return nil
        }
        if e.Response.LastLogIndex < lastLogIndex {
            // peer log has not caught up with us leader yet
            event := self.SetupReplicating(peerHSM)
            peerHSM.SelfDispatch(event)
        } else {
            // peer log has caught up with us leader already
            sm.QTran(StateLeaderPeerID)
        }
        return nil
    case ev.EventTimeoutHeartbeat:
        event := self.SetupReplicating(peerHSM)
        peerHSM.SelfDispatch(event)
    case ev.EventPeerEnterSnapshotMode:
        self.Debug("about to enter snapshot mode peer")
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
    lastSnapshotTerm := uint64(0)
    lastSnapshotIndex := uint64(0)
    lastSnapshotMeta, err := local.StateMachine().LastSnapshotInfo()
    if err == ps.ErrorNoSnapshot {
        lastSnapshotTerm = 0
        lastSnapshotIndex = 0
    } else if err != nil {
        errorEvent := ev.NewPersistErrorEvent(
            errors.New("fail to read last snapshot info"))
        local.SendPrior(errorEvent)
        return errorEvent
    } else {
        lastSnapshotTerm = lastSnapshotMeta.LastIncludedTerm
        lastSnapshotIndex = lastSnapshotMeta.LastIncludedIndex
    }
    switch {
    case (matchIndex == 0) && (!leaderPeerState.GetMatchIndexUpdated()):
        committedIndex, err := local.Log().CommittedIndex()
        if err != nil {
            message := fmt.Sprintf(
                "fail to read committed index of log, error: %s", err)
            errorEvent := ev.NewPersistErrorEvent(errors.New(message))
            local.SendPrior(errorEvent)
            return errorEvent
        }
        lastLogIndex, err := local.Log().LastIndex()
        if err != nil {
            message := fmt.Sprintf(
                "fail to read last index of log, error: %s", err)
            errorEvent := ev.NewPersistErrorEvent(errors.New(message))
            local.SendPrior(errorEvent)
            return errorEvent
        }
        logEntry, err := local.Log().GetLog(lastLogIndex)
        if err != nil {
            message := fmt.Sprintf(
                "fail to read log at index: %d, error: %s", lastLogIndex, err)
            errorEvent := ev.NewPersistErrorEvent(errors.New(message))
            local.SendPrior(errorEvent)
            return errorEvent
        }
        var prevLogTerm uint64
        var prevLogIndex uint64
        if lastLogIndex == 1 {
            prevLogTerm = 0
            prevLogIndex = 0
        } else if (lastSnapshotIndex + 1) == lastLogIndex {
            prevLogTerm = lastSnapshotTerm
            prevLogIndex = lastSnapshotIndex
        } else {
            prevLogIndex = lastLogIndex - 1
            prevLogEntry, err := local.Log().GetLog(prevLogIndex)
            if err != nil {
                message := fmt.Sprintf(
                    "fail to read log at index: %d, error: %s",
                    prevLogIndex, err)
                errorEvent := ev.NewPersistErrorEvent(errors.New(message))
                local.SendPrior(errorEvent)
                return errorEvent
            }
            prevLogTerm = prevLogEntry.Term
        }
        request := &ev.AppendEntriesRequest{
            Term:              local.GetCurrentTerm(),
            Leader:            local.GetLocalAddr(),
            PrevLogTerm:       prevLogTerm,
            PrevLogIndex:      prevLogIndex,
            Entries:           []*ps.LogEntry{logEntry},
            LeaderCommitIndex: committedIndex,
        }
        self.Debug("SetupReplicating() matchIndex == 0, AE, Term: %d, "+
            "PrevLogTerm: %d, PrevLogIndex: %d, Entries size: %d, "+
            "LeaderCommitIndex: %d, entries info: %s",
            request.Term, request.PrevLogTerm, request.PrevLogIndex,
            len(request.Entries), request.LeaderCommitIndex,
            "["+strings.Join(EntriesInfo(request.Entries), ",")+"]")
        event = ev.NewAppendEntriesRequestEvent(request)
    case matchIndex < lastSnapshotIndex:
        event = ev.NewPeerEnterSnapshotModeEvent()
    case matchIndex == 0:
        event = self.SetupNextAppendEntriesRequestEvent(
            local, uint64(0), uint64(0), nextIndex)
    default:
        log, err := local.Log().GetLog(matchIndex)
        if err != nil {
            message := fmt.Sprintf("fail to read log at index: %d, error: %s",
                matchIndex, err)
            errorEvent := ev.NewPersistErrorEvent(errors.New(message))
            local.SendPrior(errorEvent)
            return errorEvent
        }
        prevLogTerm := log.Term
        prevLogIndex := log.Index
        event = self.SetupNextAppendEntriesRequestEvent(
            local, prevLogTerm, prevLogIndex, nextIndex)
    }
    return event
}

func (self *StandardModePeerState) SetupNextAppendEntriesRequestEvent(
    local Local,
    prevLogTerm uint64,
    prevLogIndex uint64,
    fromIndex uint64) hsm.Event {

    lastLogIndex, err := local.Log().LastIndex()
    if err != nil {
        errorEvent := ev.NewPersistErrorEvent(
            errors.New("fail to read last log index of log"))
        local.SendPrior(errorEvent)
        return errorEvent
    }
    entriesSize := Min(uint64(self.maxAppendEntriesSize),
        (lastLogIndex - fromIndex + 1))
    maxIndex := fromIndex + entriesSize - 1
    logEntries, err := local.Log().GetLogInRange(fromIndex, maxIndex)
    if err != nil {
        message := fmt.Sprintf("fail to read log at range[%d, %d], error: %s",
            fromIndex, maxIndex, err)
        errorEvent := ev.NewPersistErrorEvent(errors.New(message))
        local.SendPrior(errorEvent)
        return errorEvent
    }
    committedIndex, err := local.Log().CommittedIndex()
    if err != nil {
        errorEvent := ev.NewPersistErrorEvent(
            errors.New("fail to read committed index of log"))
        local.SendPrior(errorEvent)
        return errorEvent
    }
    request := &ev.AppendEntriesRequest{
        Term:              local.GetCurrentTerm(),
        Leader:            local.GetLocalAddr(),
        PrevLogTerm:       prevLogTerm,
        PrevLogIndex:      prevLogIndex,
        Entries:           logEntries,
        LeaderCommitIndex: committedIndex,
    }
    self.Debug("SetupNextAppendEntriesRequestEvent() fromIndex == %d, AE, "+
        "Term: %d, PrevLogTerm: %d, PrevLogIndex: %d, Entries size: %d, "+
        "LeaderCommitIndex: %d, entries info: %s",
        fromIndex, request.Term, request.PrevLogTerm, request.PrevLogIndex,
        len(request.Entries), request.LeaderCommitIndex,
        "["+strings.Join(EntriesInfo(request.Entries), ",")+"]")
    return ev.NewAppendEntriesRequestEvent(request)
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

    self.Debug("STATE: %s, -> Entry", self.ID())
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    snapshotMetas, err := local.StateMachine().AllSnapshotInfo()
    if err != nil {
        local.SendPrior(ev.NewPersistErrorEvent(errors.New(
            "fail to list snapshot")))
        return nil
    }
    if len(snapshotMetas) == 0 {
        // no snapshot at all
        local.SendPrior(ev.NewPersistErrorEvent(errors.New(
            "fail to read last log index of log")))
        peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
        return nil
    }

    id := snapshotMetas[0].ID
    meta, readCloser, err := local.StateMachine().OpenSnapshot(id)
    if err != nil {
        message := fmt.Sprintf("fail to open snapshot, id: %s", id)
        local.SendPrior(ev.NewPersistErrorEvent(errors.New(message)))
        peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
        return nil
    }
    self.snapshotMeta = meta
    self.snapshotReadCloser = readCloser
    self.offset = 0
    self.lastChunk = make([]byte, 0)

    if err := self.SendNextChunk(
        peerHSM, local.GetCurrentTerm(), local.GetLocalAddr()); err != nil {

        peerAddr := peerHSM.Addr()
        self.Error("fail to send snapshot next chunk to peer: %s, offset: %d",
            peerAddr.String(), self.offset)
        peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
    }
    return nil
}

func (self *SnapshotModePeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    if err := self.snapshotReadCloser.Close(); err != nil {
        self.Error("fail to close snapshot reader for snapshot id: %s",
            self.snapshotMeta.ID)
    }
    self.snapshotMeta = nil
    self.offset = 0
    self.lastChunk = nil
    return nil
}

func (self *SnapshotModePeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    local := peerHSM.Local()
    switch event.Type() {
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(ev.Event)
        hsm.AssertTrue(ok)
        peerAddr := peerHSM.Addr()
        respEvent, err := peerHSM.Client().CallRPCTo(peerAddr, e)
        if err != nil {
            self.Error(
                "fail to call rpc InstallSnapshotRequest to peer: %s, error: %s",
                peerAddr.String(), err)
            // retry again
            peerHSM.SelfDispatch(event)
        }
        snapshotRespEvent, ok := respEvent.(*ev.InstallSnapshotResponseEvent)
        if !ok {
            self.Error("receive non InstallSnapshotResponse for " +
                "InstallSnapshotRequest")
            return nil
        }

        // update last contact timer
        leaderPeerState, ok := self.Super().(*LeaderPeerState)
        hsm.AssertTrue(ok)
        leaderPeerState.UpdateLastContact()

        if snapshotRespEvent.Response.Term > local.GetCurrentTerm() {
            local.SendPrior(ev.NewStepdownEvent())
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
                self.Debug("done send all chunk of snapshot, id: %s",
                    self.snapshotMeta.ID)
                sm.QTran(StateStandardModePeerID)
                return nil
            }

            // send next chunk
            if err := self.SendNextChunk(
                peerHSM, local.GetCurrentTerm(), local.GetLocalAddr()); err != nil {

                self.Error("fail to send snapshot next chunk to peer: %s, offset: %d",
                    peerAddr.String(), self.offset)
                peerHSM.SelfDispatch(ev.NewPeerAbortSnapshotModeEvent())
            }
        } else {
            // resend last chunk
            e := self.SetupRequest(local.GetCurrentTerm(), local.GetLocalAddr())
            peerHSM.SelfDispatch(e)
        }
        return nil
    case ev.EventPeerAbortSnapshotMode:
        self.Debug("about to exit snapshot mode peer")
        sm.QTran(StateStandardModePeerID)
        return nil
    }
    return self.Super()
}

func (self *SnapshotModePeerState) SetupRequest(
    term uint64, leader *ps.ServerAddress) hsm.Event {

    request := &ev.InstallSnapshotRequest{
        Term:              term,
        Leader:            leader,
        LastIncludedIndex: self.snapshotMeta.LastIncludedIndex,
        LastIncludedTerm:  self.snapshotMeta.LastIncludedTerm,
        Offset:            self.offset,
        Data:              self.lastChunk,
        Conf:              self.snapshotMeta.Conf,
        Size:              self.snapshotMeta.Size,
    }
    event := ev.NewInstallSnapshotRequestEvent(request)
    return event
}

func (self *SnapshotModePeerState) SendNextChunk(
    peerHSM *PeerHSM, term uint64, leader *ps.ServerAddress) error {

    data := make([]byte, self.maxSnapshotChunkSize)
    n, err := self.snapshotReadCloser.Read(data)
    if n > 0 {
        self.lastChunk = data[:n]
        requestEvent := self.SetupRequest(term, leader)
        peerHSM.SelfDispatch(requestEvent)
        return nil
    } else {
        if err == io.EOF || err == nil {
            return nil
        }
        message := fmt.Sprintf(
            "fail to read next chunk of snapshot, id: %s, error: %s",
            self.snapshotMeta.ID, err)
        return errors.New(message)
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
        ev.EventString(event))
    // TODO add impl
    return self.Super()
}

type PersistErrorPeerState struct {
    *LogStateHead
}

func NewPersistErrorPeerState(
    super hsm.State, logger logging.Logger) *PersistErrorPeerState {

    object := &PersistErrorPeerState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*PersistErrorPeerState) ID() string {
    return StatePersistErrorPeerID
}

func (self *PersistErrorPeerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *PersistErrorPeerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *PersistErrorPeerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    return self.Super()
}
