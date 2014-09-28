package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/zonas/rafted/event"
    logging "github.com/zonas/rafted/logging"
    ps "github.com/zonas/rafted/persist"
    "sync"
    "time"
)

type FollowerState struct {
    *LogStateHead

    // election timeout and its time ticker
    electionTimeout                 time.Duration
    electionTimeoutThresholdPersent float64
    electionTimeoutThreshold        time.Duration
    maxTimeoutJitter                float32
    ticker                          Ticker
    // last time we have contact from the leader
    lastContactTime     time.Time
    lastContactTimeLock sync.RWMutex
}

func NewFollowerState(
    super hsm.State,
    electionTimeout time.Duration,
    electionTimeoutThresholdPersent float64,
    maxTimeoutJitter float32,
    logger logging.Logger) *FollowerState {

    threshold := time.Duration(
        float64(electionTimeout) * electionTimeoutThresholdPersent)
    object := &FollowerState{
        LogStateHead:                    NewLogStateHead(super, logger),
        electionTimeout:                 electionTimeout,
        electionTimeoutThresholdPersent: electionTimeoutThresholdPersent,
        electionTimeoutThreshold:        threshold,
        ticker: NewRandomTicker(electionTimeout, maxTimeoutJitter),
    }
    super.AddChild(object)
    return object
}

func (*FollowerState) ID() string {
    return StateFollowerID
}

func (self *FollowerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // init global status
    localHSM.SetVotedFor(ps.NilServerAddr)
    // init status for this status
    self.UpdateLastContactTime()
    // start heartbeat timeout ticker
    onTimeout := func() {
        timeout := &ev.Timeout{
            LastTime: self.LastContactTime(),
            Timeout:  self.electionTimeout,
        }
        localHSM.SelfDispatch(ev.NewElectionTimeoutEvent(timeout))
    }
    self.ticker.Start(onTimeout)
    return nil
}

func (self *FollowerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // stop heartbeat timeout ticker
    self.ticker.Stop()
    // cleanup global status
    localHSM.SetVotedFor(ps.NilServerAddr)
    return nil
}

func (self *FollowerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        self.Debug(
            "follower receive RequestVoteRequestEvent %#v, local term: %d",
            e.Request, localHSM.GetCurrentTerm())
        self.UpdateLastContact(localHSM)
        // Update to latest term if we see newer term
        if e.Request.Term > localHSM.GetCurrentTerm() {
            localHSM.SetCurrentTermWithNotify(e.Request.Term)
            // the old leader is now invalidated
            localHSM.SetLeader(ps.NilServerAddr)
            localHSM.SelfDispatch(event)
            localHSM.QTran(StateFollowerID)
            return nil
        }
        response := self.HandleRequestVoteRequest(localHSM, e.Request)
        e.SendResponse(ev.NewRequestVoteResponseEvent(response))
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        hsm.AssertTrue(ok)
        self.Debug("follower receive AppendEntriesRequest: %#v, local term: %d",
            e.Request, localHSM.GetCurrentTerm())
        // Update to latest term if we see newer term
        if e.Request.Term > localHSM.GetCurrentTerm() {
            localHSM.SetCurrentTermWithNotify(e.Request.Term)
            localHSM.SetLeaderWithNotify(e.Request.Leader)
            localHSM.SelfDispatch(event)
            localHSM.QTran(StateFollowerID)
            return nil
        }
        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(
                errors.New("fail to read last entry index")))
            // don't know how to response the request without lastLogIndex
            return nil
        }
        response := self.HandleAppendEntriesRequest(
            localHSM, e.Request, lastLogIndex)
        self.Debug("response AE with Term: %d, LastLogIndex: %d, Success: %t",
            response.Term, response.LastLogIndex, response.Success)
        e.SendResponse(ev.NewAppendEntriesResponseEvent(response))
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        // transfer to snapshot recovery state and
        // replay this event on its entry/init/handle handlers.
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        term := localHSM.GetCurrentTerm()
        response := &ev.InstallSnapshotResponse{
            Term:    term,
            Success: false,
        }
        if e.Request.Term < term {
            // ignore stale request
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }
        // Update to latest term if we see newer term
        if e.Request.Term > term {
            localHSM.SetCurrentTermWithNotify(e.Request.Term)
            localHSM.SetLeaderWithNotify(e.Request.Leader)
            localHSM.SelfDispatch(event)
            localHSM.QTran(StateFollowerID)
            return nil
        }

        if e.Request.Offset == 0 {
            // only transfer to snapshot recovery state when receive first chunk
            localHSM.QTranOnEvent(StateSnapshotRecoveryID, event)
        } else {
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }
        return nil
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.RequestEvent)
        hsm.AssertTrue(ok)
        // redirect client to current leader
        leader := localHSM.GetLeader()
        if ps.AddrEqual(&leader, &ps.NilServerAddr) {
            e.SendResponse(ev.NewLeaderUnknownResponseEvent())
        } else {
            response := &ev.LeaderRedirectResponse{leader}
            e.SendResponse(ev.NewLeaderRedirectResponseEvent(response))
        }
        return nil
    case event.Type() == ev.EventTimeoutElection:
        e, ok := event.(*ev.ElectionTimeoutEvent)
        hsm.AssertTrue(ok)
        localHSM.Notifier().Notify(ev.NewNotifyElectionTimeoutEvent(
            e.Message.LastTime, e.Message.Timeout))
        localHSM.Notifier().Notify(ev.NewNotifyStateChangeEvent(
            ev.RaftStateFollower, ev.RaftStateCandidate))
        localHSM.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventMemberChangeNextStep:
        e, ok := event.(*ev.MemberChangeNextStepEvent)
        hsm.AssertTrue(ok)
        conf := e.Message.Conf
        if !(ps.IsOldNewConfig(conf) &&
            localHSM.GetMemberChangeStatus() == NotInMemeberChange) {
            DispatchInconsistantError(localHSM)
            return nil
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            message := fmt.Sprintf(
                "fail to read last index of log, error: %#v", err)
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
        }

        nextLogIndex := lastLogIndex + 1
        err = localHSM.ConfigManager().Push(nextLogIndex, conf)
        if err != nil {
            DispatchPushConfigError(localHSM, nextLogIndex)
            return nil
        }
        localHSM.SetMemberChangeStatus(OldNewConfigSeen)
        sm.QTran(StateFollowerOldNewConfigSeenID)
        return nil
    }
    return self.Super()
}

func (self *FollowerState) HandleRequestVoteRequest(
    localHSM *LocalHSM,
    request *ev.RequestVoteRequest) *ev.RequestVoteResponse {

    term := localHSM.GetCurrentTerm()
    response := &ev.RequestVoteResponse{
        Term:    term,
        Granted: false,
    }

    // Ignore any older term
    if request.Term < term {
        self.Debug("ignore RequestVoteRequest with older term: %d, "+
            "current term: %d", request.Term, term)
        return response
    }

    votedFor := localHSM.GetVotedFor()
    if ps.AddrNotEqual(&votedFor, &ps.NilServerAddr) {
        // already voted once before
        if ps.AddrNotEqual(&votedFor, &request.Candidate) {
            self.Info("reject vote for term: %d, candidate: %s",
                request.Term, request.Candidate.String())
            return response
        }
        self.Warning("re-vote for term: %d, candidate: %s",
            request.Term, request.Candidate.String())
    }

    // Reject if the candiate's logs are not at least as up-to-date as ours.
    lastTerm, lastIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(
            errors.New("fail to read last entry info of log")))
        return response
    }
    if lastTerm > request.LastLogTerm {
        self.Info("receive stale request vote, term: %d, candidate: %s",
            request.Term, request.Candidate.String())
        return response
    }
    if lastIndex > request.LastLogIndex {
        self.Info("receive stale request vote, index: %d, candidate: %s",
            request.LastLogIndex, request.Candidate.String())
        return response
    }

    localHSM.SetVotedFor(request.Candidate)
    response.Granted = true
    return response
}

func (self *FollowerState) HandleAppendEntriesRequest(
    localHSM *LocalHSM,
    request *ev.AppendEntriesRequest,
    lastLogIndex uint64) *ev.AppendEntriesResponse {

    term := localHSM.GetCurrentTerm()
    response := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: lastLogIndex,
        Success:      false,
    }

    // Ignore any older term
    if request.Term < term {
        self.Debug("ignore RequestVoteRequest with older term: %d, "+
            "current term: %d", request.Term, term)
        return response
    }

    leader := localHSM.GetLeader()
    if ps.AddrEqual(&leader, &ps.NilServerAddr) {
        localHSM.SetLeaderWithNotify(request.Leader)
        self.UpdateLastContact(localHSM)
    } else if ps.AddrEqual(&leader, &request.Leader) {
        self.UpdateLastContact(localHSM)
    } else {
        // two leader in the same term sending AppendEntriesRequest' to us
        self.Error("receive request at term: %d from bot leader: %s, %s",
            term, leader.String(), request.Leader.String())
        return response
    }

    if !self.checkPrevIndex(
        localHSM, request.PrevLogIndex, request.PrevLogTerm) {
        return response
    }
    if !checkIndexesForEntries(&request.Entries) {
        self.Info("append entries request log index not in ascending order")
        return response
    }

    log := localHSM.Log()
    // store any new entries
    if n := len(request.Entries); n > 0 {
        first := request.Entries[0]
        // delete any conflicting entries
        if first.Index <= lastLogIndex {
            self.Info("AppendEntriesRequest log entry start from "+
                "index: %d, less than local last log index: %d",
                first.Index, lastLogIndex)
            if err := log.TruncateAfter(first.Index); err != nil {
                message := fmt.Sprintf(
                    "fail to truncate log after index: %d, error: %s",
                    first.Index, err)
                e := errors.New(message)
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
                return response
            }
        }

        if err := log.StoreLogs(request.Entries); err != nil {
            message := fmt.Sprintf(
                "fail to store logs from index: %d to index: %d",
                first.Index, request.Entries[n-1].Index)
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
            return response
        }

        newLastIndex, err := log.LastIndex()
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last log index")))
            return response
        }
        response.LastLogIndex = newLastIndex
    }

    // Update the commit index
    committedIndex, err := log.CommittedIndex()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read committed index of log")))
        return response
    }
    if (request.LeaderCommitIndex > 0) &&
        (request.LeaderCommitIndex > committedIndex) {
        index := Min(request.LeaderCommitIndex, lastLogIndex)
        if err = localHSM.CommitLogsUpTo(index); err != nil {
            message := fmt.Sprintf(
                "fail to commit log up to index: %d, error: %s", index, err)
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
            return response
        }
        self.Debug("*** update commit index from %d to %d", committedIndex, index)
        fromIndex := Min(committedIndex+1, index)
        self.Debug("*** notify commit from index %d to %d", fromIndex, index)
        entries, err := localHSM.Log().GetLogInRange(fromIndex, index)
        if err != nil {
            message := fmt.Sprintf("fail to read log in range [%d, %d]",
                fromIndex, index)
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
            return response
        }
        for i := fromIndex; i <= index; i++ {
            entry := entries[i-fromIndex]
            localHSM.Notifier().Notify(ev.NewNotifyCommitEvent(
                entry.Term, entry.Index))
        }
    }

    // dispatch member change events
    if !self.dispatchMemberChangeEvents(localHSM, committedIndex, lastLogIndex) {
        return response
    }
    response.Success = true
    return response
}

func (self *FollowerState) LastContactTime() time.Time {
    self.lastContactTimeLock.RLock()
    defer self.lastContactTimeLock.RUnlock()
    return self.lastContactTime
}

func (self *FollowerState) UpdateLastContactTime() {
    self.lastContactTimeLock.Lock()
    defer self.lastContactTimeLock.Unlock()
    self.lastContactTime = time.Now()
}

func (self *FollowerState) UpdateLastContact(localHSM *LocalHSM) {
    lastContactTime := self.LastContactTime()
    if TimeExpire(lastContactTime, self.electionTimeoutThreshold) {
        localHSM.Notifier().Notify(ev.NewNotifyElectionTimeoutThresholdEvent(
            lastContactTime, self.electionTimeout))
    }
    self.UpdateLastContactTime()
    self.ticker.Reset()
}

// check PrevLogIndex, PrevLogTerm in AppendEntriesRequest with local log.
func (self *FollowerState) checkPrevIndex(
    localHSM *LocalHSM, prevLogIndex, prevLogTerm uint64) bool {

    var logTerm uint64 = 0
    lastTerm, lastIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        message := fmt.Sprintf(
            "fail to read last entry info of log, error: %s", err)
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
        return false
    }
    if prevLogIndex == lastIndex {
        logTerm = lastTerm
    } else {
        prevLog, err := localHSM.Log().GetLog(prevLogIndex)
        if err != nil {
            self.Warning("fail to read log at index: %d, error: %s", prevLogIndex, err)
            return false
        }
        logTerm = prevLog.Term
    }

    if prevLogTerm != logTerm {
        self.Info("inconsistant log in append entries request, "+
            "index: %d, term: %d, while local index: %d, term: %d",
            prevLogIndex, prevLogTerm, prevLogIndex, logTerm)
        return false
    }
    return true
}

// check log entry index increase ascendingly
func checkIndexesForEntries(entries *[]*ps.LogEntry) bool {
    var index uint64 = 0
    for _, entry := range *entries {
        if !(index < entry.Index) {
            return false
        }
        index = entry.Index
    }
    return true
}

func (self *FollowerState) dispatchMemberChangeEvents(
    localHSM *LocalHSM, committedIndex, lastLogIndex uint64) bool {

    log := localHSM.Log()
    newLastLogIndex, err := log.LastIndex()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read last log index of log")))
        return false
    }
    newCommittedIndex, err := log.CommittedIndex()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
            "fail to read committed index of log")))
        return false
    }

    // No more log is appended or commited, in this case:
    // committedIndex == newCommittedIndex == lastLogIndex == newLastLogIndex
    if committedIndex == newLastLogIndex {
        return true
    }

    logEntries, err := log.GetLogInRange(committedIndex+1, newLastLogIndex)
    if err != nil {
        message := fmt.Sprintf("fail to read log in range [%d, %d]",
            committedIndex+1, newLastLogIndex)
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
        return false
    }

    if newCommittedIndex <= lastLogIndex {
        // from old CommittedIndex to new CommittedIndex, if any member change entry commit
        // send MemberChangeLogEntryCommitEvent
        for i := committedIndex + 1; i <= newCommittedIndex; i++ {
            entry := logEntries[i-(committedIndex+1)]
            if entry.Type == ps.LogMemberChange {
                message := &ev.MemberChangeNewConf{entry.Conf}
                localHSM.SelfDispatch(ev.NewMemberChangeLogEntryCommitEvent(message))
            }
        }
        // from old LastLogIndex to new LastLogIndex, if any member change entry commit
        // send MemberChangeNextStepEvent
        for i := lastLogIndex + 1; i <= newLastLogIndex; i++ {
            entry := logEntries[i-(committedIndex+1)]
            if entry.Type == ps.LogMemberChange {
                message := &ev.MemberChangeNewConf{entry.Conf}
                localHSM.SelfDispatch(ev.NewMemberChangeNextStepEvent(message))
            }
        }
    } else {
        // from old CommittedIndex to old LastLogIndex, if any member change entry commit
        // send MemberChangeLogEntryCommitEvent
        for i := committedIndex + 1; i <= lastLogIndex; i++ {
            entry := logEntries[i-(committedIndex+1)]
            if entry.Type == ps.LogMemberChange {
                message := &ev.MemberChangeNewConf{entry.Conf}
                localHSM.SelfDispatch(ev.NewMemberChangeLogEntryCommitEvent(message))
            }
        }
        // from old LastLogIndex to new CommittedIndex, if any member change entry commit
        // send MemberChangeNextStepEvent and MemberChangeLogEntryCommitEvent
        for i := lastLogIndex + 1; i <= newCommittedIndex; i++ {
            entry := logEntries[i-(committedIndex+1)]
            if entry.Type == ps.LogMemberChange {
                message := &ev.MemberChangeNewConf{entry.Conf}
                localHSM.SelfDispatch(ev.NewMemberChangeNextStepEvent(message))
                localHSM.SelfDispatch(ev.NewMemberChangeLogEntryCommitEvent(message))
            }
        }
        // from new CommittedIndex to new LastLogIndex, if any member change entry commit
        // send MemberChangeNextStepEvent
        for i := newCommittedIndex + 1; i <= newLastLogIndex; i++ {
            entry := logEntries[i-(committedIndex+1)]
            if entry.Type == ps.LogMemberChange {
                message := &ev.MemberChangeNewConf{entry.Conf}
                localHSM.SelfDispatch(ev.NewMemberChangeNextStepEvent(message))
            }
        }
    }
    return true
}

func DispatchInconsistantError(localHSM *LocalHSM) error {
    message := "config and member change status inconsistant"
    e := errors.New(message)
    localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
    return e
}

func DispatchPushConfigError(localHSM *LocalHSM, logIndex uint64) error {
    message := fmt.Sprintf(
        "fail to push new config for log at index: %d", logIndex)
    e := errors.New(message)
    localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
    return e
}
