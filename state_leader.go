package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
)

type LeaderState struct {
    *LogStateHead

    MemberChangeHSM *LeaderMemberChangeHSM
    Inflight        *Inflight
}

func NewLeaderState(super hsm.State, logger logging.Logger) *LeaderState {
    object := &LeaderState{
        LogStateHead:    NewLogStateHead(super, logger),
        MemberChangeHSM: SetupLeaderMemberChangeHSM(logger),
    }
    object.MemberChangeHSM.SetLeaderState(object)
    super.AddChild(object)
    return object
}

func (*LeaderState) ID() string {
    return StateLeaderID
}

func (self *LeaderState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // init global status
    localHSM.SetLeaderWithNotify(localHSM.GetLocalAddr())
    // coordinate peer into LeaderPeerState
    localHSM.PeerManager().Broadcast(ev.NewPeerEnterLeaderEvent())
    // init status for this state
    conf, err := localHSM.ConfigManager().RNth(0)
    if err != nil {
        // TODO error handling
    }
    self.Inflight = NewInflight(conf)
    // activate member change hsm
    self.MemberChangeHSM.SetLocalHSM(localHSM)
    self.MemberChangeHSM.Dispatch(ev.NewLeaderMemberChangeActivateEvent())
    return nil
}

func (self *LeaderState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    sm.QInit(StateUnsyncID)
    return nil
}

func (self *LeaderState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // cleanup status for this state
    self.Inflight.Init()
    // deactivate member change hsm
    self.MemberChangeHSM.Dispatch(ev.NewLeaderMemberChangeDeactivateEvent())
    return nil
}

func (self *LeaderState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        peerUpdate := &ev.PeerReplicateLog{
            Peer:       localHSM.GetLocalAddr(),
            MatchIndex: e.Response.LastLogIndex,
        }
        localHSM.SelfDispatch(ev.NewPeerReplicateLogEvent(peerUpdate))
        return nil
    case ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        // step down to follower state if local term is not greater than
        // the remote one
        if e.Request.Term >= localHSM.GetCurrentTerm() {
            ReplayEventAndStepdown(
                localHSM, event, e.Request.Term, e.Request.Leader)
        }
        return nil
    case ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        if e.Request.Term >= localHSM.GetCurrentTerm() {
            ReplayEventAndStepdown(
                localHSM, event, e.Request.Term, e.Request.Candidate)
        }
        return nil
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        if e.Request.Term >= localHSM.GetCurrentTerm() {
            ReplayEventAndStepdown(
                localHSM, event, e.Request.Term, e.Request.Leader)
        }
        return nil
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(*ev.ClientReadOnlyRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(
            localHSM, e.Request.Data, e.ClientRequestEventHead.ResultChan)
        return nil
    case ev.EventClientWriteRequest:
        e, ok := event.(*ev.ClientWriteRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(
            localHSM, e.Request.Data, e.ClientRequestEventHead.ResultChan)
        return nil
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
            self.CommitInflightEntries(localHSM, allCommitted)
        }
        return nil
    case ev.EventStepdown:
        localHSM.Notifier().Notify(ev.NewNotifyStateChangeEvent(
            ev.RaftStateLeader, ev.RaftStateFollower))
        sm.QTran(StateFollowerID)
        return nil
    case ev.EventClientMemberChangeRequest:
        fallthrough
    case ev.EventLeaderReenterMemberChangeState:
        fallthrough
    case ev.EventLeaderForwardMemberChangePhase:
        self.MemberChangeHSM.Dispatch(event)
        return nil
    }
    return self.Super()
}

func (self *LeaderState) HandleClientRequest(
    localHSM *LocalHSM, requestData []byte, resultChan chan ev.ClientEvent) {

    conf, err := localHSM.ConfigManager().RNth(0)
    if err != nil {
        // TODO error handling
    }
    requests := &InflightRequest{
        LogType:    ps.LogCommand,
        Data:       requestData,
        Conf:       conf,
        ResultChan: resultChan,
    }
    if err := self.StartFlight(localHSM, requests); err != nil {
        // TODO error handling
    }
}

func (self *LeaderState) StartFlight(
    localHSM *LocalHSM, request *InflightRequest) error {

    term := localHSM.GetCurrentTerm()
    lastLogIndex, err := localHSM.Log().LastIndex()
    if err != nil {
        // TODO add error handling
    }

    // construct durable log entry
    logIndex := lastLogIndex + 1
    // bundled config with log entry if in member change procedure
    var conf *ps.Config
    if ps.IsInMemeberChange(request.Conf) {
        conf = request.Conf
    }
    logEntry := &ps.LogEntry{
        Term:  term,
        Index: logIndex,
        Type:  request.LogType,
        Data:  request.Data,
        Conf:  conf,
    }

    // persist log locally
    if err := localHSM.Log().StoreLog(logEntry); err != nil {
        // TODO error handling
        response := &ev.ClientResponse{
            Success: false,
        }
        event := ev.NewClientResponseEvent(response)
        request.SendResponse(event)
    }

    if ps.IsInMemeberChange(request.Conf) {
        localHSM.PeerManager().ResetPeers(localHSM.GetLocalAddr(), request.Conf)
        self.Inflight.ChangeMember(request.Conf)
    }
    //  and inflight log entry
    self.Inflight.Add(logIndex, request)

    // send AppendEntriesReqeust to all peer
    lastLogTerm, lastLogIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        // TODO error handling
    }
    committedIndex, err := localHSM.Log().CommittedIndex()
    if err != nil {
        // TODO error handling
    }

    // retrieve all uncommitted logs
    logEntries, err := localHSM.Log().GetLogInRange(
        committedIndex+1, lastLogIndex)
    if err != nil {
        // TODO error handling
    }
    req := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            localHSM.GetLocalAddr(),
        PrevLogIndex:      lastLogIndex,
        PrevLogTerm:       lastLogTerm,
        Entries:           logEntries,
        LeaderCommitIndex: committedIndex,
    }
    event := ev.NewAppendEntriesRequestEvent(req)

    selfResponse := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: lastLogIndex,
        Success:      true,
    }
    localHSM.SelfDispatch(ev.NewAppendEntriesResponseEvent(selfResponse))
    localHSM.PeerManager().Broadcast(event)
    return nil
}

func (self *LeaderState) CommitInflightEntries(
    localHSM *LocalHSM, entries []*InflightEntry) {

    for _, entry := range entries {
        if entry.Request.LogType == ps.LogMemberChange {
            _, err := localHSM.CommitLogAt(entry.LogIndex)
            if err != nil {
                // TODO error handling
            }
            // don't response client here
            message := &ev.LeaderForwardMemberChangePhase{
                Conf:       entry.Request.Conf,
                ResultChan: entry.Request.ResultChan,
            }
            localHSM.SelfDispatch(
                ev.NewLeaderForwardMemberChangePhaseEvent(message))
        } else {
            result, err := localHSM.CommitLogAt(entry.LogIndex)
            if err != nil {
                // TODO error handling
            }
            // response client immediately
            response := &ev.ClientResponse{
                Success: true,
                Data:    result,
            }
            entry.Request.SendResponse(ev.NewClientResponseEvent(response))
        }
    }
}

type UnsyncState struct {
    *LogStateHead

    noop     *InflightRequest
    listener *ClientEventListener
}

func NewUnsyncState(super hsm.State, logger logging.Logger) *UnsyncState {
    ch := make(chan ev.ClientEvent, 1)
    object := &UnsyncState{
        LogStateHead: NewLogStateHead(super, logger),
        noop: &InflightRequest{
            LogType:    ps.LogNoop,
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

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    handleNoopResponse := func(event ev.ClientEvent) {
        if event.Type() != ev.EventClientResponse {
            // TODO add log
            return
        }
        localHSM.SelfDispatch(event)
    }
    self.listener.Start(handleNoopResponse)
    self.StartSyncSafe(localHSM)
    return nil
}

func (self *UnsyncState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    return self.Super()
}

func (self *UnsyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    self.listener.Stop()
    return nil
}

func (self *UnsyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        e.SendResponse(ev.NewLeaderUnsyncResponseEvent())
        return nil
    case ev.EventClientResponse:
        e, ok := event.(*ev.ClientResponseEvent)
        hsm.AssertTrue(ok)
        // TODO add different policy for retry
        if e.Response.Success {
            localHSM.QTran(StateSyncID)
        } else {
            self.StartSyncSafe(localHSM)
        }
        return nil
    }
    return self.Super()
}

func (self *UnsyncState) StartSync(localHSM *LocalHSM) error {
    // commit a blank no-op entry into the log at the start of leader's term
    leaderState, ok := self.Super().(*LeaderState)
    hsm.AssertTrue(ok)
    return leaderState.StartFlight(localHSM, self.noop)
}

func (self *UnsyncState) StartSyncSafe(localHSM *LocalHSM) {
    if err := self.StartSync(localHSM); err != nil {
        // TODO add log
        localHSM.SelfDispatch(ev.NewStepdownEvent())
    }
}

type SyncState struct {
    *LogStateHead
}

func NewSyncState(super hsm.State, logger logging.Logger) *SyncState {
    object := &SyncState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*SyncState) ID() string {
    return StateSyncID
}

func (self *SyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *SyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *SyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    return self.Super()
}
