package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "sync"
    "time"
)

type CandidateState struct {
    *hsm.StateHead

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
    super hsm.State, electionTimeout time.Duration) *CandidateState {

    object := &CandidateState{
        StateHead:       hsm.NewStateHead(super),
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

    fmt.Println(self.ID(), "-> Entry")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    // init global status
    raftHSM.SetLeader(nil)
    // init status for this state
    self.UpdateLastElectionTime()
    self.grantedVoteCount = 0
    // start election procedure
    self.StartElection(raftHSM)
    // start election timeout ticker
    notifyElectionTimeout := func() {
        if TimeExpire(self.LastElectionTime(), self.electionTimeout) {
            raftHSM.SelfDispatch(ev.NewElectionTimeoutEvent())
        }
    }
    self.ticker.Start(notifyElectionTimeout)
    return nil
}

func (self *CandidateState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // stop election timeout ticker
    self.ticker.Stop()
    // cleanup status for this state
    self.grantedVoteCount = 0
    return nil
}

func (self *CandidateState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event=", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutElection:
        // transfer to self, trigger `Exit' and `Entry'
        raftHSM.QTran(StateCandidateID)
        return nil
    case event.Type() == ev.EventRequestVoteResponse:
        e, ok := event.(*ev.RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        if e.Response.Term > raftHSM.GetCurrentTerm() {
            // TODO add log
            raftHSM.SetCurrentTerm(e.Response.Term)
            raftHSM.QTran(StateFollowerID)
            return nil
        }

        if e.Response.Granted {
            // TODO add log
            self.grantedVoteCount++
        }

        if self.grantedVoteCount >= raftHSM.QuorumSize() {
            // TODO add log
            raftHSM.QTran(StateLeaderID)
        }
        return nil
    case event.Type() == ev.EventAppendEntriesRequest:
        e, ok := event.(*ev.AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        // step down to follower state if term if equal or newer than
        // local current term
        if e.Request.Term >= raftHSM.GetCurrentTerm() {
            // discover a new leader, transfer to follower state

            // replay this event in future
            raftHSM.SelfDispatch(event)
            // record the leader
            leader, err := DecodeAddr(e.Request.Leader)
            if err != nil {
                // TODO add err checking
            }
            raftHSM.SetLeader(leader)
            raftHSM.QTran(StateFollowerID)
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

func (self *CandidateState) StartElection(raftHSM *RaftHSM) {
    // increase the term
    raftHSM.SetCurrentTerm(raftHSM.GetCurrentTerm() + 1)

    // Vote for self
    term := raftHSM.GetCurrentTerm()
    localAddrBin, err := EncodeAddr(raftHSM.LocalAddr)
    if err != nil {
        // TODO error handling
    }
    request := &ev.RequestVoteRequest{
        Term:         term,
        Candidate:    localAddrBin,
        LastLogIndex: raftHSM.GetLastIndex(),
        LastLogTerm:  raftHSM.GetLastTerm(),
    }
    event := ev.NewRequestVoteRequestEvent(request)

    voteMyselfResponse := &ev.RequestVoteResponse{
        Term:    term,
        Granted: true,
    }
    raftHSM.SelfDispatch(ev.NewRequestVoteResponseEvent(voteMyselfResponse))
    raftHSM.SetVotedFor(raftHSM.LocalAddr)

    // broadcast RequestVote RPCs to all other servers
    raftHSM.PeerManager.Broadcast(event)
}
