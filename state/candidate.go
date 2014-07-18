package state

import "fmt"
import "time"
import "sync"
import hsm "github.com/hhkbp2/go-hsm"

type Candidate struct {
    *hsm.StateHead

    // election timeout and its time ticker
    electionTimeout time.Duration
    ticker          Ticker
    // last time we have start election
    lastElectionTime     time.Time
    lastElectionTimeLock sync.RWMutex
    // vote
    grantedVoteCount
}

func NewCandidate(super hsm.State, electionTimeout time.Duration) *Candidate {
    object := &Candidate{
        StateHead:       hsm.NewStateHead(super),
        electionTimeout: electionTimeout,
        ticker:          NewRandomTicker(electionTimeout),
    }
    super.AddChild(object)
    return object
}

func (*Candidate) ID() string {
    return CandidateID
}

func (self *Candidate) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    // init global status
    raftHSM.SetLeader(nil)
    // init status for this state
    self.UpdateLastElectionTime()
    self.grantedVoteCount = 0
    // start election procedure
    self.StartElection()
    // start election timeout ticker
    notifyElectionTimeout := func() {
        if TimeExpire(self.LastElectionTime(), self.electionTimeout) {
            raftHSM.SelfDispatch(NewElectionTimeoutEvent())
        }
    }
    self.ticker.Start(notifyElectionTimeout)
    return nil
}

func (self *Candidate) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    // stop election timeout ticker
    self.ticker.Stop()
    // cleanup status for this state
    self.grantedVoteCount = 0
    return nil
}

func (self *Candidate) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event=", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == EventTimeoutElection:
        // transfer to self, trigger `Exit' and `Entry'
        raftHSM.QTran(CandidateID)
        return nil
    case event.Type() == EventRequestVoteResponse:
        e, ok := event.(*RequestVoteResponseEvent)
        hsm.AssertTrue(ok)
        if e.Response.Term > raftHSM.GetCurrentTerm() {
            // TODO add log
            raftHSM.SetCurrentTerm(e.Response.Term)
            QTran(FollowerID)
            return nil
        }

        if e.Response.Granted {
            // TODO add log
            self.grantedVoteCount++
        }

        if self.grantedVoteCount >= raftHSM.QuorumSize() {
            // TODO add log
            QTran(LeaderID)
        }
        return nil
    case IsClientEvent(event.Type()):
        // Don't know whether there is a leader or who is leader.
        // Return a error response.
        e, ok := event.(*RequestEvent)
        hsm.AssertTrue(ok)
        response := &LeaderUnknownResponse{}
        e.Response(NewLeaderUnknownResponseEvent(response))
        return nil
    }
    return self.Super()
}

func (self *Candidate) LastElectionTime() time.Time {
    self.lastElectionTimeLock.RLock()
    defer self.lastElectionTimeLock.RUnlock()
    return self.lastElectionTime
}

func (self *Candidate) UpdateLastElectionTime() {
    self.lastElectionTimeLock.Lock()
    defer self.lastElectionTimeLock.Unlock()
    self.lastElectionTime = time.Now()
}

func (self *Candidate) StartElection(raftHSM *RaftHSM) {
    // increase the term
    raftHSM.SetCurrentTerm(raftHSM.GetCurrentTerm() + 1)

    // broadcast RequestVoteRequest to all peers
    localAddrBin, err := EncodeAddr(raftHSM.LocalAddr)
    hsm.AssertNil(err)
    request := &RequestVoteRequest{
        Term:         raftHSM.GetCurrentTerm(),
        Candidate:    localAddrBin,
        LastLogIndex: raftHSM.GetLastIndex(),
        LastLogTerm:  raftHSM.GetLastTerm(),
    }
    event := NewRequestVoteRequestEvent(request)
    raftHSM.PeerManager.Broadcast(event)
    voteMyselfResponse := &RequestVoteResponse{
        Term:    request.Term,
        Granted: True,
    }
    raftHSM.SelfDispatch(NewRequestVoteResponseEvent(voteMyselfResponse))
}
