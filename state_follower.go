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

func NewFollowerState(super hsm.State, heartbeatTimeout time.Duration) *FollowerState {
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

func (self *FollowerState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    // init status for this status
    self.UpdateLastContactTime()
    // start heartbeat timeout ticker
    hsm.AssertEqual(HSMTypeRaft, sm.Type())
    raftHSM, ok := sm.(SelfDispatchHSM)
    hsm.AssertTrue(ok)
    notifyHeartbeatTimeout := func() {
        if TimeExpire(self.LastContactTime(), self.heartbeatTimeout) {
            raftHSM.SelfDispatch(ev.NewHeartbeatTimeoutEvent())
        }
    }
    self.ticker.Start(notifyHeartbeatTimeout)
    return nil
}

func (self *FollowerState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    return self.Super()
}

func (self *FollowerState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    // stop heartbeat timeout ticker
    self.ticker.Stop()
    return nil
}

func (self *FollowerState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event =", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutHeartBeat:
        raftHSM.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventRequestVoteRequest:
        e, ok := event.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        response := self.ProcessRequestVote(raftHSM, e.Request)
        e.SendResponse(ev.NewRequestVoteResponseEvent(response))
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        response := self.ProcessAppendEntries(raftHSM, e.Request)
        e.SendResponse(ev.NewAppendEntriesResponseEvent(response))
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        // TODO add substate
        return nil
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.RequestEvent)
        hsm.AssertTrue(ok)
        // redirect client to current leader
        leader := raftHSM.GetLeader().String()
        response := &ev.LeaderRedirectResponse{leader}
        e.SendResponse(ev.NewLeaderRedirectResponseEvent(response))
        return nil
    }
    return self.Super()
}

func (self *FollowerState) ProcessRequestVote(
    raftHSM *RaftHSM,
    request *ev.RequestVoteRequest) *ev.RequestVoteResponse {

    // TODO initialize peers correctly
    term := raftHSM.GetCurrentTerm()
    response := &ev.RequestVoteResponse{
        Term:    term,
        Granted: false,
    }

    // Ignore any older term
    if request.Term < term {
        return response
    }

    // Update to latest term if we see newer term
    if request.Term > term {
        raftHSM.SetCurrentTerm(request.Term)
        response.Term = request.Term
        // No need to call QTran("Follower")
        // since we are already in follwer state
    }

    votedFor := raftHSM.GetVotedFor()
    votedForBin, err := EncodeAddr(votedFor)
    hsm.AssertNil(err)
    if (votedFor != nil) &&
        (bytes.Compare(votedForBin, request.Candidate) != 0) {
        // TODO add log
        return response
    }

    // Reject if the candiate's logs are not at least as up-to-date as ours.
    lastTerm, lastIndex := raftHSM.GetLastLogInfo()
    if lastTerm > request.LastLogTerm {
        // TODO add log
        return response
    }
    if lastIndex > request.LastLogIndex {
        // TODO add log
        return response
    }

    response.Granted = true
    return response
}

func (self *FollowerState) ProcessAppendEntries(
    raftHSM *RaftHSM,
    request *ev.AppendEntriesRequest) *ev.AppendEntriesResponse {

    term := raftHSM.GetCurrentTerm()
    response := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: raftHSM.GetLastIndex(),
        Success:      false,
    }

    // Ignore any older term
    if request.Term < term {
        return response
    }

    // Update to latest term if we see newer term
    if request.Term > term {
        raftHSM.SetCurrentTerm(request.Term)
        response.Term = request.Term
        // No need to call QTran("Follower")
        // since we are already in follwer state
    }

    // update current leader on every request
    leader, err := DecodeAddr(request.Leader)
    hsm.AssertNil(err)
    raftHSM.SetLeader(leader)

    if request.PrevLogIndex > 0 {
        lastTerm, lastIndex := raftHSM.GetLastLogInfo()

        var prevLogTerm uint64
        if request.PrevLogIndex == lastIndex {
            prevLogTerm = lastTerm
        } else {
            prevLog, err := raftHSM.GetLog().GetLog(request.PrevLogIndex)
            if err != nil {
                // TODO add log
                return response
            } else {
                prevLogTerm = prevLog.Term
            }
        }

        if request.PrevLogTerm != prevLogTerm {
            // TODO add log
            return response
        }
    }

    // Process any new entries
    if n := len(request.Entries); n > 0 {
        first := request.Entries[0]

        // Delete any conflicting entries
        lastLogIndex := raftHSM.GetLastIndex()
        if first.Index <= lastLogIndex {
            // TODO add log
            if err := raftHSM.GetLog().TruncateAfter(first.Index); err != nil {
                // TODO add log
                return response
            }
        }

        // Append the entry
        if err := raftHSM.GetLog().StoreLogs(request.Entries); err != nil {
            // TODO add log
            return response
        }
    }

    // Update the commit index
    if (request.LeaderCommitIndex > 0) &&
        (request.LeaderCommitIndex > raftHSM.GetCommitIndex()) {
        index := min(request.LeaderCommitIndex, raftHSM.GetLastIndex())
        raftHSM.SetCommitIndex(index)
        raftHSM.ApplyLogs()
    }

    response.Success = true
    self.UpdateLastContactTime()
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
