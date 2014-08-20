package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "sync"
    "time"
)

type FollowerState struct {
    *LogStateHead

    // election timeout and its time ticker
    electionTimeout                 time.Duration
    electionTimeoutThresholdPersent float32
    ticker                          Ticker
    // last time we have contact from the leader
    lastContactTime     time.Time
    lastContactTimeLock sync.RWMutex
}

func NewFollowerState(
    super hsm.State,
    electionTimeout time.Duration,
    electionTimeoutThresholdPersent float32,
    logger logging.Logger) *FollowerState {

    object := &FollowerState{
        LogStateHead:                    NewLogStateHead(super, logger),
        electionTimeout:                 electionTimeout,
        electionTimeoutThresholdPersent: electionTimeoutThresholdPersent,
        ticker: NewRandomTicker(electionTimeout),
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
    dispatchTimeout := func() {
        lastContactTime := self.LastContactTime()
        if TimeExpire(lastContactTime, self.electionTimeout) {
            timeout := &ev.Timeout{
                LastTime: lastContactTime,
                Timeout:  self.electionTimeout,
            }
            localHSM.SelfDispatch(ev.NewElectionTimeoutEvent(timeout))
        }
    }
    self.ticker.Start(dispatchTimeout)
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
        ev.EventTypeString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        self.UpdateLastContact(localHSM)
        response, toSend := self.ProcessRequestVote(localHSM, e.Request)
        if toSend {
            e.SendResponse(ev.NewRequestVoteResponseEvent(response))
        } else {
            localHSM.SelfDispatch(event)
        }
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        response, toSend := self.ProcessAppendEntries(localHSM, e.Request)
        if toSend {
            e.SendResponse(ev.NewAppendEntriesResponseEvent(response))
        } else {
            localHSM.SelfDispatch(event)
        }
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        // transfer to snapshot recovery state and
        // replay this event on its entry/init/handle handlers.
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        if (e.Request.Term >= localHSM.GetCurrentTerm()) &&
            (e.Request.Offset == 0) {
            localHSM.QTranOnEvent(StateSnapshotRecoveryID, event)
        }
        return nil
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        // redirect client to current leader
        response := &ev.LeaderRedirectResponse{localHSM.GetLeader()}
        e.SendResponse(ev.NewLeaderRedirectResponseEvent(response))
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
            // TODO error handling
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }

        err = localHSM.ConfigManager().PushConfig(lastLogIndex+1, conf)
        if err != nil {
            // TODO error handling
        }
        localHSM.SetMemberChangeStatus(OldNewConfigSeen)
        sm.QTran(StateFollowerOldNewConfigSeenID)
        return nil
    }
    return self.Super()
}

func (self *FollowerState) ProcessRequestVote(
    localHSM *LocalHSM,
    request *ev.RequestVoteRequest) (*ev.RequestVoteResponse, bool) {

    // TODO initialize peers correctly
    term := localHSM.GetCurrentTerm()
    response := &ev.RequestVoteResponse{
        Term:    term,
        Granted: false,
    }

    // Ignore any older term
    if request.Term < term {
        return response, true
    }

    // Update to latest term if we see newer term
    if request.Term > term {
        localHSM.SetCurrentTerm(request.Term)
        // the old leader is now invalidated
        localHSM.SetLeader(ps.NilServerAddr)
        // transfer to follwer state for a new term
        localHSM.QTran(StateFollowerID)
        return nil, false
    }

    votedFor := localHSM.GetVotedFor()
    if ps.AddrNotEqual(&votedFor, &ps.NilServerAddr) {
        // already voted once before
        if ps.AddrEqual(&votedFor, &request.Candidate) {
            // TODO add log
            response.Granted = true
        }
        // TODO add log
        return response, true
    }

    // Reject if the candiate's logs are not at least as up-to-date as ours.
    lastTerm, lastIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        // TODO error handling
    }
    if lastTerm > request.LastLogTerm {
        // TODO add log
        return response, true
    }
    if lastIndex > request.LastLogIndex {
        // TODO add log
        return response, true
    }

    localHSM.SetVotedFor(request.Candidate)
    response.Granted = true
    return response, true
}

func (self *FollowerState) ProcessAppendEntries(
    localHSM *LocalHSM,
    request *ev.AppendEntriesRequest) (*ev.AppendEntriesResponse, bool) {

    log := localHSM.Log()
    term := localHSM.GetCurrentTerm()
    lastLogTerm, lastLogIndex, err := log.LastEntryInfo()
    if err != nil {
        // TODO error handling
    }
    response := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: lastLogIndex,
        Success:      false,
    }

    // Ignore any older term
    if request.Term < term {
        return response, true
    }

    leader := localHSM.GetLeader()
    // Update to latest term if we see newer term
    if request.Term > term {
        localHSM.SetCurrentTerm(request.Term)
        // update leader in new term
        localHSM.SetLeader(request.Leader)
        // transfer to follower state for a new term
        localHSM.QTran(StateFollowerID)
        return nil, false
    }

    if ps.AddrEqual(&leader, &ps.NilServerAddr) {
        localHSM.SetLeader(request.Leader)
        self.UpdateLastContact(localHSM)
    } else if ps.AddrEqual(&leader, &request.Leader) {
        self.UpdateLastContact(localHSM)
    } else {
        // TODO error handling
        // fatal error two leader in the same term sending AE to us
    }

    if request.PrevLogIndex > 0 {
        var prevLogTerm uint64
        if request.PrevLogIndex == lastLogIndex {
            prevLogTerm = lastLogTerm
        } else {
            prevLog, err := log.GetLog(request.PrevLogIndex)
            if err != nil {
                // TODO error handling
                return response, true
            } else {
                prevLogTerm = prevLog.Term
            }
        }

        if request.PrevLogTerm != prevLogTerm {
            // TODO error handling
            return response, true
        }
    }

    // check log entry index increase ascendingly
    err = checkIndexesForEntries(&request.Entries)
    if err != nil {
        // TODO add log
        return response, true
    }

    // Process any new entries
    if n := len(request.Entries); n > 0 {
        first := request.Entries[0]
        // Delete any conflicting entries
        if first.Index <= lastLogIndex {
            // TODO add log
            if err := log.TruncateAfter(first.Index); err != nil {
                // TODO add log
                return response, true
            }
        }

        // Append the entry
        if err := log.StoreLogs(request.Entries); err != nil {
            // TODO add log
            return response, true
        }
    }

    // Update the commit index
    committedIndex, err := log.CommittedIndex()
    if err != nil {
        // TODO error handling
    }
    if (request.LeaderCommitIndex > 0) &&
        (request.LeaderCommitIndex > committedIndex) {
        index := Min(request.LeaderCommitIndex, lastLogIndex)
        localHSM.CommitLogsUpTo(index)
        log.StoreCommittedIndex(index)
    }

    // dispatch member change events
    newLastLogIndex, err := log.LastIndex()
    if err != nil {
        // TODO error handling
    }
    newCommittedIndex, err := log.CommittedIndex()
    if err != nil {
        // TODO error handling
    }
    logEntries, err := log.GetLogInRange(committedIndex+1, newLastLogIndex)
    if err != nil {
        // TODO error handling
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
    response.Success = true
    return response, true
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
    if TimeExpire(lastContactTime, self.electionTimeout) {
        localHSM.Notifier().Notify(ev.NewNotifyElectionTimeoutThresholdEvent(
            lastContactTime, self.electionTimeout))
    }
    self.UpdateLastContactTime()
    self.ticker.Reset()
}

func checkIndexesForEntries(entries *[]*ps.LogEntry) error {
    var index uint64 = 0
    for _, entry := range *entries {
        if !(index < entry.Index) {
            return errors.New("log entry index not ascending")
        }
        index = entry.Index
    }
    return nil
}
