package raft_example

import "fmt"
import "time"
import "sync"
import "net"
import "bytes"
import hsm "github.com/hhkbp2/go-hsm"

const (
    FollowerID  = "follower"
    CandidateID = "candidate"
    LeaderID    = "leader"
)

type Follower struct {
    hsm.StateHead
    // heartbeat timeout and its time ticker
    heartbeatTimeout time.Duration
    ticker           Ticker
    // last time we have contact from the leader
    lastContactTime time.Time
    lastContactLock sync.RWMutex
}

func NewFollower(super hsm.State, heartbeatTimeout time.Duration) *Follower {
    object := &Follower{
        StateHead:        hsm.MakeStateHead(super),
        heartbeatTimeout: heartbeatTimeout,
        ticker:           NewRandomTicker(heartbeatTimeout),
    }
    super.AddChild(object)
    return object
}

func (*Follower) ID() string {
    return FollowerID
}

func (self *Follower) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    self.UpdateLastContactTime()
    hsm.AssertEqual(HSMTypeRaft, sm.Type())
    raftHSM, ok := sm.(SelfDispatchHSM)
    hsm.AssertTrue(ok)
    notifyHeartbeatTimeout := func() {
        if TimeExpire(self.LastContactTime(), self.heartbeatTimeout) {
            raftHSM.SelfDispatch(NewHeartbeatTimeoutEvent())
        }
    }
    self.ticker.Start(notifyHeartbeatTimeout)
    return nil
}

func (self *Follower) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    raftHSM.SetLeader(nil)
    self.ticker.Stop()
    return nil
}

func (self *Follower) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event =", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == EventTimeoutHeartBeat:
        hsm.QTran("Candidate")
        return nil
    case event.Type() == EventRequestVoteRequest:
        e, ok := event.(*RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        response := self.ProcessRequestVote(raftHSM, e.Request)
        e.ResultChan <- NewRequestVoteResponseEvent(response)
        return nil
    case event.Type() == EventAppendEntriesRequest:
        e, ok := event.(*AppendEntriesReqeustEvent)
        hsm.AssertTrue(ok)
        response := self.ProcessAppendEntries(raftHSM, e.Request)
        e.ResultChan <- NewAppendEntriesResponseEvent(response)
        return nil
    case event.Type() == EventPrepareInstallSnapshotRequest:
        // TODO add substate
        return nil
    case event.Type() == EventInstallSnapshotRequest:
        // TODO add substate
        return nil
    case IsClientEvent(event.Type()):
        e, ok := event.(*RequestEvent)
        hsm.AssertTrue(ok)
        // redirect client to current leader
        leader := raftHSM.GetLeader().String()
        response := &RedirectResponse{leader}
        e.ResultChan <- NewRedirectResponseEvent(response)
        return nil
    }
    return self.Super()
}

func (self *Follower) ProcessRequestVote(raftHSM *RaftHSM, request *RequestVoteRequest) *RequestVoteResponse {
    // TODO initialize peers correctly
    term := raftHSM.GetCurrentTerm()
    response := &RequestVoteResponse{
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
        // No need to call QTran("Follower") since we are already in follwer state
    }

    votedFor := raftHSM.GetVotedFor()
    votedForBin, err := EncodeAddr(votedFor)
    hsm.AssertNil(err)
    if (votedFor != nil) && (bytes.Compare(votedForBin, request.Candidate) != 0) {
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

func (self *Follower) ProcessAppendEntries(raftHSM *RaftHSM, request *AppendEntriesRequest) *AppendEntriesResponse {
    term := raftHSM.GetCurrentTerm()
    response := &AppendEntriesResponse{
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
        // No need to call QTran("Follower") since we are already in follwer state
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
            if prevLog, err := raftHSM.GetLog().GetLog(request.PrevLogIndex); err != nil {
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
            if err := raftHSM.GetLog().Truncate(first.Index); err != nil {
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
    if (request.LeaderCommitIndex > 0) && (request.LeaderCommitIndex > raftHSM.GetCommitIndex()) {
        index := min(request.LeaderCommitIndex, raftHSM.GetLastIndex())
        raftHSM.SetCommitIndex(index)
        raftHSM.ApplyLogs()
    }

    response.Success = true
    self.UpdateLastContactTime()
    return response
}

func (self *Follower) LastContactTime() time.Time {
    self.lastContactLock.RLock()
    defer self.lastContactLock.RUnlock()
    return self.lastContactTime
}

func (self *Follower) UpdateLastContactTime() {
    self.lastContactLock.Lock()
    defer self.lastContactLock.Unlock()
    self.lastContactTime = time.Now()
}

type Candidate struct {
    hsm.StateHead
    // election timeout and its time ticker
    electionTimeout time.Duration
    ticker          Ticker
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
    // TODO add
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
        hsm.QTran("Candidate")
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

func (self *Candidate) StartElection()

type Leader struct {
    hsm.StateHead
}

func NewLeader(super hsm.State) *Leader {
    object := &Leader{hsm.MakeStateHead(super)}
    super.AddChild(object)
    return object
}

func (*Leader) ID() string {
    return LeaderID
}

func (self *Leader) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *Leader) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *Leader) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event=", event)
    switch event.Type() {
    case EventRequestVoteRequest:
        // TODO DEBUG
        fmt.Println("Leader possible step down")
        return nil
    case EventReadRequest:
        fmt.Println("Leader process request")
        return nil
    }
    return self.Super()
}

func CreateRaftHSM(heartbeatTimeout time.Duration) TerminableHSM {
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, FollowerID)
    NewFollower(top, heartbeatTimeout)
    NewCandidate(top)
    NewLeader(top)
    raftHSM := NewRaftHSM(top, initial)
    raftHSM.Init()
    return raftHSM
}
