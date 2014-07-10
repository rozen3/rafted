package state

import "fmt"
import "time"
import "sync"
import hsm "github.com/hhkbp2/go-hsm"

type Candidate struct {
    hsm.StateHead
    // election timeout and its time ticker
    electionTimeout time.Duration
    ticker          Ticker
    // last time we have start election
    lastElectionTime     time.Time
    lastElectionTimeLock sync.RWMutex
}

func NewCandidate(super hsm.State, electionTimeout time.Duration) *Candidate {
    object := &Candidate{
        StateHead:       hsm.MakeStateHead(super),
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
    raftHSM.SetLeader(nil)
    // TODO start election procedure
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
    self.ticker.Stop()
    return nil
}

func (self *Candidate) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event=", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == EventTimeoutElection:
        // transfer to self, trigger `Exit' and `Entry'
        raftHSM.QTran("Candidate")
        return nil
    case event.Type() == EventRequestVoteResponse:
        // TODO DEBUG
        fmt.Println("Candidate needs to handle vote")
        return nil
    case IsClientEvent(event.Type()):
        // Don't know whether there is a leader or who is leader.
        // Return a error response.
        e, ok := event.(*RequestEvent)
        hsm.AssertTrue(ok)
        response := &LeaderUnknownResponse{}
        e.ResultChan <- NewLeaderUnknownResponseEvent(response)
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

func (self *Candidate) StartElection() {
    // TODO add impl
}
