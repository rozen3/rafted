package rafted

import (
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
    grantedVoteCount uint32
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
    // init status for this state
    self.UpdateLastElectionTime()
    self.grantedVoteCount = 0
    // start election procedure
    self.StartElection(localHSM)
    // start election timeout ticker
    notifyElectionTimeout := func() {
        if TimeExpire(self.LastElectionTime(), self.electionTimeout) {
            localHSM.SelfDispatch(ev.NewElectionTimeoutEvent())
        }
    }
    self.ticker.Start(notifyElectionTimeout)
    return nil
}

func (self *CandidateState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // stop election timeout ticker
    self.ticker.Stop()
    // cleanup status for this state
    self.grantedVoteCount = 0
    return nil
}

func (self *CandidateState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.PrintEvent(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutElection:
        // transfer to self, trigger Exit and Entry
        localHSM.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventRequestVoteResponse:
        e, ok := event.(*ev.RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        if e.Response.Term > localHSM.GetCurrentTerm() {
            // TODO add log
            localHSM.SetCurrentTerm(e.Response.Term)
            localHSM.QTran(StateFollowerID)
            return nil
        }

        if e.Response.Granted {
            // TODO add log
            self.grantedVoteCount++
        }

        // add inflight
        // if self.grantedVoteCount >= localHSM.ConfigManager().QuorumSize() {
        //     // TODO add log
        //     localHSM.QTran(StateLeaderID)
        // }
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        // step down to follower state if local term is not greater than
        // the remote one
        if e.Request.Term >= localHSM.GetCurrentTerm() {
            Stepdown(localHSM, event, e.Request.Term, e.Request.Leader)
        }
        return nil
    case ev.IsClientEvent(event.Type()):
        // Don't know whether there is a leader or who is leader.
        // Return a error response.
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.LeaderUnknownResponse{}
        e.SendResponse(ev.NewLeaderUnknownResponseEvent(response))
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
    localHSM.SetCurrentTerm(localHSM.GetCurrentTerm() + 1)

    // Vote for self
    term := localHSM.GetCurrentTerm()
    lastLogTerm, lastLogIndex, err := localHSM.Log().LastEntryInfo()
    if err != nil {
        // TODO error handling
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

func Stepdown(localHSM *LocalHSM, event hsm.Event, term uint64, leader ps.ServerAddr) {
    // replay this event
    localHSM.SelfDispatch(event)
    // record the term
    localHSM.SetCurrentTerm(term)
    // record the leader
    localHSM.SetLeader(leader)
    localHSM.QTran(StateFollowerID)
}
