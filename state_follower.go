package rafted

import (
    "bytes"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "sync"
    "time"
)

type FollowerState struct {
    *hsm.StateHead

    // heartbeat timeout and its time ticker
    heartbeatTimeout time.Duration
    ticker           Ticker
    // last time we have contact from the leader
    lastContactTime     time.Time
    lastContactTimeLock sync.RWMutex
}

func NewFollowerState(
    super hsm.State, heartbeatTimeout time.Duration) *FollowerState {

    object := &FollowerState{
        StateHead:        hsm.NewStateHead(super),
        heartbeatTimeout: heartbeatTimeout,
        ticker:           NewRandomTicker(heartbeatTimeout),
    }
    super.AddChild(object)
    return object
}

func (*FollowerState) ID() string {
    return StateFollowerID
}

func (self *FollowerState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // init global status
    localHSM.SetVotedFor(nil)
    // init status for this status
    self.UpdateLastContactTime()
    // start heartbeat timeout ticker
    deliverHeartbeatTimeout := func() {
        lastContactTime := self.LastContactTime()
        if TimeExpire(lastContactTime, self.heartbeatTimeout) {
            timeout := &HeartbeatTimeout{
                LastContactTime: lastContactTime,
                Timeout:         self.heartbeatTimeout,
            }
            localHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent(timeout))
        }
    }
    self.ticker.Start(deliverHeartbeatTimeout)
    return nil
}

func (self *FollowerState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // stop heartbeat timeout ticker
    self.ticker.Stop()
    // cleanup global status
    localHSM.SetVotedFor(nil)
    return nil
}

func (self *FollowerState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event =", event)
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutHeartBeat:
        localHSM.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        self.UpdateLastContact()
        response, toSend := self.ProcessRequestVote(localHSM, e.Request)
        if toSend {
            e.SendResponse(ev.NewRequestVoteResponseEvent(response))
        }
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        self.UpdateLastContact()
        response, toSend := self.ProcessAppendEntries(localHSM, e.Request)
        if toSend {
            e.SendResponse(ev.NewAppendEntriesResponseEvent(response))
        }
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        // transfer to snapshot recovery state and
        // replay this event on its entry/init/handle handlers.
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        self.UpdateLastContact()
        if (e.Request.Term >= localHSM.GetCurrentTerm()) &&
            (e.Request.Offset == 0) {
            localHSM.QTranOnEvent(StateSnapshotRecoveryID, event)
        }
        return nil
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        // redirect client to current leader
        leader := localHSM.GetLeader().String()
        response := &ev.LeaderRedirectResponse{leader}
        e.SendResponse(ev.NewLeaderRedirectResponseEvent(response))
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
        localHSM.SetLeader(nil)
        // transfer to follwer state for a new term
        localHSM.SelfDispatch(event)
        localHSM.QTran(StateFollowerID)
        return nil, false
    }

    votedFor := localHSM.GetVotedFor()
    if votedFor != nil {
        // already voted once before
        votedForBin, err := EncodeAddr(votedFor)
        hsm.AssertNil(err)
        if bytes.Compare(votedForBin, request.Candidate) == 0 {
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

    candidate, err := DecodeAddr(request.Candidate)
    if err != nil {
        // TODO add err checking
    }
    localHSM.SetVotedFor(candidate)
    response.Granted = true
    return response, true
}

func (self *FollowerState) ProcessAppendEntries(
    localHSM *LocalHSM,
    request *ev.AppendEntriesRequest) (*ev.AppendEntriesResponse, bool) {

    term := localHSM.GetCurrentTerm()
    response := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: localHSM.Log().LastIndex(),
        Success:      false,
    }

    // Ignore any older term
    if request.Term < term {
        return response, true
    }

    // decode leader from request
    leader, err := DecodeAddr(request.Leader)
    if err != nil {
        // TODO error handling
    }

    // Update to latest term if we see newer term
    if request.Term > term {
        localHSM.SetCurrentTerm(request.Term)
        // update leader in new term
        localHSM.SetLeader(leader)
        // transfer to follower state for a new term
        localHSM.SelfDispatch(event)
        localHSM.QTran(StateFollowerID)
        return nil, false
    }

    if localHSM.GetLeader() == nil {
        localHSM.SetLeader(leader)
        self.UpdateLastContact()
    } else if persist.AddrEqual(localHSM.GetLeader(), leader) {
        self.UpdateLastContact()
    } else {
        // TODO error handling
        // fatal error two leader in the same term sending AE to us
    }

    if request.PrevLogIndex > 0 {
        lastTerm, lastIndex := localHSM.Log().LastEntryInfo()

        var prevLogTerm uint64
        if request.PrevLogIndex == lastIndex {
            prevLogTerm = lastTerm
        } else {
            prevLog, err := localHSM.Log().GetLog(request.PrevLogIndex)
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

    // Process any new entries
    if n := len(request.Entries); n > 0 {
        first := request.Entries[0]

        // Delete any conflicting entries
        lastLogIndex := localHSM.Log().LastIndex()
        if first.Index <= lastLogIndex {
            // TODO add log
            if err := localHSM.Log().TruncateAfter(first.Index); err != nil {
                // TODO add log
                return response, true
            }
        }

        // Append the entry
        if err := localHSM.Log().StoreLogs(request.Entries); err != nil {
            // TODO add log
            return response, true
        }
    }

    // Update the commit index
    if (request.LeaderCommitIndex > 0) &&
        (request.LeaderCommitIndex > localHSM.Log().CommitIndex()) {
        index := min(request.LeaderCommitIndex, localHSM.Log().LastIndex())
        localHSM.Log().SetCommitIndex(index)
        localHSM.ProcessLogsUpTo(index)
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

func (self *FollowerState) UpdateLastContact() {
    self.UpdateLastContactTime()
    self.ticker.Reset()
}
