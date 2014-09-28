package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/zonas/rafted/event"
    logging "github.com/zonas/rafted/logging"
    ps "github.com/zonas/rafted/persist"
    "strings"
)

type LeaderState struct {
    *LogStateHead

    MemberChangeHSM *LeaderMemberChangeHSM
    Inflight        *Inflight
    listener        *ClientEventListener
}

func NewLeaderState(super hsm.State, logger logging.Logger) *LeaderState {
    object := &LeaderState{
        LogStateHead:    NewLogStateHead(super, logger),
        MemberChangeHSM: SetupLeaderMemberChangeHSM(logger),
        listener:        NewClientEventListener(),
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
    localHSM.Peers().Broadcast(ev.NewPeerEnterLeaderEvent())
    // activate member change hsm
    self.MemberChangeHSM.SetLocalHSM(localHSM)
    self.MemberChangeHSM.Dispatch(ev.NewLeaderMemberChangeActivateEvent())
    ignoreResponse := func(event ev.Event) {
        e, ok := event.(*ev.ClientResponseEvent)
        hsm.AssertTrue(ok)
        self.Info("orphan client response: %t", e.Response.Success)
    }
    self.listener.Start(ignoreResponse)
    // init status for this state
    conf, err := localHSM.ConfigManager().RNth(0)
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read last config")))
        return nil
    }
    self.Inflight = NewInflight(conf)
    committedIndex, err := localHSM.Log().CommittedIndex()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read committed index of log")))
        return nil
    }
    lastLogIndex, err := localHSM.Log().LastIndex()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read last log index of log")))
    }
    if committedIndex < lastLogIndex {
        inflightEntries := make([]*InflightEntry, 0, lastLogIndex-committedIndex)
        for i := committedIndex + 1; i <= lastLogIndex; i++ {
            logEntry, err := localHSM.Log().GetLog(i)
            if err != nil {
                message := fmt.Sprintf("fail to get log at index: %d", i)
                e := errors.New(message)
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
            }
            request := &InflightRequest{
                LogEntry:   logEntry,
                ResultChan: self.listener.GetChan(),
            }
            inflightEntry := NewInflightEntry(request)
            inflightEntries = append(inflightEntries, inflightEntry)
        }
        self.Inflight.AddAll(inflightEntries)
        // TODO check for AddAll()
    }
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
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // cleanup status for this state
    self.Inflight.Init()
    self.listener.Stop()
    // deactivate member change hsm
    self.MemberChangeHSM.Dispatch(ev.NewLeaderMemberChangeDeactivateEvent())
    // cleanup global status
    localHSM.SetLeader(ps.NilServerAddr)
    return nil
}

func (self *LeaderState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventAppendEntriesResponse:
        e, ok := event.(*ev.AppendEntriesResponseEvent)
        hsm.AssertTrue(ok)
        self.Debug("leader receive AppendEntriesResponse: %#v", e.Response)
        peerUpdate := &ev.PeerReplicateLog{
            Peer:       localHSM.GetLocalAddr(),
            MatchIndex: e.Response.LastLogIndex,
        }
        localHSM.SelfDispatch(ev.NewPeerReplicateLogEvent(peerUpdate))
        return nil
    case ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        hsm.AssertTrue(ok)
        self.Debug(
            "leader receive AppendEntriesRequest: %#v from: %s, local term: %d",
            e.Request, e.Request.Leader.String(), localHSM.GetCurrentTerm())
        // step down to follower state if local term is not greater than
        // the remote one
        if e.Request.Term > localHSM.GetCurrentTerm() {
            localHSM.SelfDispatch(ev.NewStepdownEvent())
            localHSM.SelfDispatch(event)
            return nil
        }
        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(
                errors.New("fail to read last index of log")))
            return nil
        }
        response := &ev.AppendEntriesResponse{
            Term:         localHSM.GetCurrentTerm(),
            LastLogIndex: lastLogIndex,
            Success:      false,
        }
        e.SendResponse(ev.NewAppendEntriesResponseEvent(response))
        return nil
    case ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        self.Debug(
            "leader receive RequestVoteRequest: %#v from: %s, local term: %d",
            e.Request, e.Request.Candidate.String(), localHSM.GetCurrentTerm())
        if e.Request.Term > localHSM.GetCurrentTerm() {
            localHSM.SelfDispatch(ev.NewStepdownEvent())
            localHSM.SelfDispatch(event)
            return nil
        }
        response := &ev.RequestVoteResponse{
            Term:    localHSM.GetCurrentTerm(),
            Granted: false,
        }
        e.SendResponse(ev.NewRequestVoteResponseEvent(response))
        return nil
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        if e.Request.Term > localHSM.GetCurrentTerm() {
            localHSM.SelfDispatch(ev.NewStepdownEvent())
            localHSM.SelfDispatch(event)
            return nil
        }
        response := &ev.InstallSnapshotResponse{
            Term:    localHSM.GetCurrentTerm(),
            Success: false,
        }
        e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
        return nil
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(*ev.ClientReadOnlyRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(localHSM, e.Request.Data, e.ResultChan)
        return nil
    case ev.EventClientAppendRequest:
        self.Debug("receive ClientAppendRequestEvent")
        e, ok := event.(*ev.ClientAppendRequestEvent)
        hsm.AssertTrue(ok)
        self.HandleClientRequest(localHSM, e.Request.Data, e.ResultChan)
        return nil
    case ev.EventPeerReplicateLog:
        e, ok := event.(*ev.PeerReplicateLogEvent)
        hsm.AssertTrue(ok)
        self.Debug("leader receive PeerReplicateLog: %#v from: %s",
            e.Message, e.Message.Peer.String())
        goodToCommit, err := self.Inflight.Replicate(
            e.Message.Peer, e.Message.MatchIndex)
        if err != nil {
            self.Error("fail to replicate, peer: %s, index: %d, error: %s",
                e.Message.Peer.String(), e.Message.MatchIndex, err)
            return nil
        }
        if goodToCommit {
            allCommitted := self.Inflight.GetCommitted()
            err = self.CommitInflightEntries(localHSM, allCommitted)
            if err != nil {
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(err))
                return nil
            }
            self.Debug("commit log index starts from %d to %d",
                allCommitted[0].Request.LogEntry.Index,
                allCommitted[len(allCommitted)-1].Request.LogEntry.Index)
        }
        return nil
    case ev.EventStepdown:
        localHSM.Notifier().Notify(ev.NewNotifyStateChangeEvent(
            ev.RaftStateLeader, ev.RaftStateFollower))
        sm.QTran(StateFollowerID)
        return nil
    case ev.EventClientChangeConfigRequest:
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
    localHSM *LocalHSM, requestData []byte, resultChan chan ev.Event) {

    err := self.StartFlight(localHSM, ps.LogCommand, requestData, resultChan)
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(err))
        resultChan <- ev.NewPersistErrorResponseEvent(err)
    }
}

func (self *LeaderState) StartFlight(
    localHSM *LocalHSM,
    logType ps.LogType,
    logData []byte,
    resultChan chan ev.Event) error {

    term := localHSM.GetCurrentTerm()
    log := localHSM.Log()
    lastLogTerm, lastLogIndex, err := log.LastEntryInfo()
    if err != nil {
        message := fmt.Sprintf(
            "fail to read last entry info of log, error: %s", err)
        return errors.New(message)
    }
    committedIndex, err := log.CommittedIndex()
    if err != nil {
        return errors.New("fail to read committed index of log")
    }

    // construct durable log entry
    logIndex := lastLogIndex + 1
    // bundled config with log entry if in member change procedure
    conf, err := localHSM.ConfigManager().RNth(0)
    if err != nil {
        return errors.New("fail to read last config")
    }
    logEntry := &ps.LogEntry{
        Term:  term,
        Index: logIndex,
        Type:  logType,
        Data:  logData,
        Conf:  conf,
    }

    // persist log locally
    if err := log.StoreLog(logEntry); err != nil {
        message := fmt.Sprintf("fail to store log, error: %s", err)
        return errors.New(message)
    }

    if ps.IsInMemeberChange(conf) {
        localHSM.Peers().AddPeers(GetPeers(localHSM.GetLocalAddr(), conf))
        self.Inflight.ChangeMember(conf)
    }

    //  and inflight log entry
    inflightRequest := &InflightRequest{
        LogEntry:   logEntry,
        ResultChan: resultChan,
    }
    self.Inflight.Add(inflightRequest)
    // TODO add check for Add()

    // send AppendEntriesReqeust to all peer
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            localHSM.GetLocalAddr(),
        PrevLogIndex:      lastLogIndex,
        PrevLogTerm:       lastLogTerm,
        Entries:           []*ps.LogEntry{logEntry},
        LeaderCommitIndex: committedIndex,
    }
    self.Debug("StartFlight() AE, Term: %d, PrevLogTerm: %d, PrevLogIndex: %d, "+
        "Entries size: %d, LeaderCommitIndex: %d, entries info: %s",
        request.Term, request.PrevLogTerm, request.PrevLogIndex,
        len(request.Entries), request.LeaderCommitIndex,
        "["+strings.Join(EntriesInfo(request.Entries), ",")+"]")
    event := ev.NewAppendEntriesRequestEvent(request)

    selfResponse := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: logIndex,
        Success:      true,
    }
    localHSM.SelfDispatch(ev.NewAppendEntriesResponseEvent(selfResponse))
    localHSM.Peers().Broadcast(event)
    return nil
}

func (self *LeaderState) CommitInflightEntries(
    localHSM *LocalHSM, entries []*InflightEntry) error {

    for _, entry := range entries {
        err := localHSM.CommitInflightLog(entry)
        if err != nil {
            return err
        }
        localHSM.Notifier().Notify(ev.NewNotifyCommitEvent(
            entry.Request.LogEntry.Term, entry.Request.LogEntry.Index))
    }
    return nil
}

type UnsyncState struct {
    *LogStateHead

    listener *ClientEventListener
}

func NewUnsyncState(super hsm.State, logger logging.Logger) *UnsyncState {
    object := &UnsyncState{
        LogStateHead: NewLogStateHead(super, logger),
        listener:     NewClientEventListener(),
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
    handleNoopResponse := func(event ev.Event) {
        if event.Type() != ev.EventClientResponse {
            self.Error("unsync receive response event: %s",
                ev.EventString(event))
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
        ev.EventString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(ev.RequestEvent)
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
    logType := ps.LogNoop
    logData := make([]byte, 0)
    resultChan := self.listener.GetChan()
    return leaderState.StartFlight(localHSM, logType, logData, resultChan)
}

func (self *UnsyncState) StartSyncSafe(localHSM *LocalHSM) {
    if err := self.StartSync(localHSM); err != nil {
        self.Error("unsync fail to start sync, error: %s", err)
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(err))
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
        ev.EventString(event))
    return self.Super()
}
