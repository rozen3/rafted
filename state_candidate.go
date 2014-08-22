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

type CandidateState struct {
    *LogStateHead

    // election timeout and its time ticker
    electionTimeout time.Duration
    ticker          Ticker
    // last time we have start election
    lastElectionTime     time.Time
    lastElectionTimeLock sync.RWMutex
    // vote
    condition CommitCondition
}

func NewCandidateState(
    super hsm.State,
    electionTimeout time.Duration,
    logger logging.Logger) *CandidateState {

    object := &CandidateState{
        LogStateHead:    NewLogStateHead(super, logger),
        electionTimeout: electionTimeout,
        ticker:          NewRandomTicker(electionTimeout),
    }
    super.AddChild(object)
    return object
}

func (*CandidateState) ID() string {
    return StateCandidateID
}

func (self *CandidateState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // init global status
    localHSM.SetLeader(ps.NilServerAddr)
    memberChangeStatus := localHSM.GetMemberChangeStatus()
    switch memberChangeStatus {
    case OldNewConfigSeen:
        fallthrough
    case OldNewConfigCommitted:
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            self.Error("fail to read config")
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(err))
            return nil
        }
        self.condition = NewMemberChangeCommitCondition(conf)
    case NewConfigSeen:
        conf, err := localHSM.ConfigManager().RNth(1)
        if err != nil {
            self.Error("fail to read config")
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(err))
            return nil
        }
        self.condition = NewMemberChangeCommitCondition(conf)
    case NotInMemeberChange:
        fallthrough
    default:
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last config")))
            return nil
        }
        self.condition = NewMajorityCommitCondition(conf.Servers)
    }
    // init status for this state
    self.UpdateLastElectionTime()
    // start election procedure
    self.StartElection(localHSM)
    // start election timeout ticker
    dispatchTimeout := func() {
        lastElectionTime := self.LastElectionTime()
        if TimeExpire(lastElectionTime, self.electionTimeout) {
            timeout := &ev.Timeout{
                LastTime: lastElectionTime,
                Timeout:  self.electionTimeout,
            }
            localHSM.SelfDispatch(ev.NewElectionTimeoutEvent(timeout))
        }
    }
    self.ticker.Start(dispatchTimeout)
    return nil
}

func (self *CandidateState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // stop election timeout ticker
    self.ticker.Stop()
    // cleanup status for this state
    self.condition = nil
    return nil
}

func (self *CandidateState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch {
    // TODO process RequestVoteRequestEvent here?
    case event.Type() == ev.EventRequestVoteResponse:
        e, ok := event.(*ev.RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        term := localHSM.GetCurrentTerm()
        if e.Response.Term < term {
            // ignore stale response for old term
            return nil
        }

        if e.Response.Term > term {
            self.Debug("candidate receive RequestVoteRequest with term: %d"+
                " > local term: %d", e.Response.Term, term)
            localHSM.SetCurrentTermWithNotify(e.Response.Term)
            localHSM.SelfDispatch(ev.NewStepdownEvent())
            return nil
        }

        if e.Response.Granted {
            self.Info("candidate receive Granted RequestVoteRequest from: %s",
                e.FromAddr.String())
            self.condition.AddVote(e.FromAddr)
            if self.condition.IsCommitted() {
                localHSM.Notifier().Notify(ev.NewNotifyStateChangeEvent(
                    ev.RaftStateCandidate, ev.RaftStateLeader))
                sm.QTran(StateLeaderID)
            }
        }
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        // step down to follower state if local term is not greater than
        // the remote one
        if e.Request.Term >= localHSM.GetCurrentTerm() {
            localHSM.SelfDispatch(ev.NewStepdownEvent())
            localHSM.SelfDispatch(event)
        }
        return nil
    case ev.IsClientEvent(event.Type()):
        // Don't know whether there is a leader or who is leader.
        // Return a error response.
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        e.SendResponse(ev.NewLeaderUnknownResponseEvent())
        return nil
    case event.Type() == ev.EventTimeoutElection:
        e, ok := event.(*ev.ElectionTimeoutEvent)
        hsm.AssertTrue(ok)
        localHSM.Notifier().Notify(ev.NewNotifyElectionTimeoutEvent(
            e.Message.LastTime, e.Message.Timeout))
        // transfer to self, trigger Exit and Entry
        sm.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventStepdown:
        localHSM.Notifier().Notify(ev.NewNotifyStateChangeEvent(
            ev.RaftStateCandidate, ev.RaftStateFollower))
        sm.QTran(StateFollowerID)
        return nil
    }
    return self.Super()
}

func (self *CandidateState) LastElectionTime() time.Time {
    self.lastElectionTimeLock.RLock()
    defer self.lastElectionTimeLock.RUnlock()
    return self.lastElectionTime
}

func (self *CandidateState) UpdateLastElectionTime() {
    self.lastElectionTimeLock.Lock()
    defer self.lastElectionTimeLock.Unlock()
    self.lastElectionTime = time.Now()
}

func (self *CandidateState) StartElection(localHSM *LocalHSM) {
    // increase the term
    localHSM.SetCurrentTermWithNotify(localHSM.GetCurrentTerm() + 1)

    // Vote for self
    term := localHSM.GetCurrentTerm()
    lastLogTerm, lastLogIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(
            errors.New("fail to read last entry info of log")))
        return
    }
    request := &ev.RequestVoteRequest{
        Term:         term,
        Candidate:    localHSM.GetLocalAddr(),
        LastLogIndex: lastLogIndex,
        LastLogTerm:  lastLogTerm,
    }
    event := ev.NewRequestVoteRequestEvent(request)

    voteMyselfResponse := &ev.RequestVoteResponse{
        Term:    term,
        Granted: true,
    }
    localHSM.SelfDispatch(ev.NewRequestVoteResponseEvent(voteMyselfResponse))
    localHSM.SetVotedFor(localHSM.GetLocalAddr())

    // broadcast RequestVote RPCs to all other servers
    localHSM.PeerManager().Broadcast(event)
}
