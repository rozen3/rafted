package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    persist "github.com/hhkbp2/rafted/persist"
)

type UnsyncState struct {
    *hsm.StateHead

    // TODO add fields
}

func NewUnsyncState(super hsm.State) *UnsyncState {
    object := &UnsyncState{hsm.NewStateHead(super)}
    super.AddChild(object)
    return object
}

func (*UnsyncState) ID() string {
    return StateUnsyncID
}

func (self *UnsyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    self.StartSync(raftHSM)
    return nil
}

func (self *UnsyncState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    return self.Super()
}

func (self *UnsyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // TODO add impl
    return nil
}

func (self *UnsyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    // TODO add impl
    _, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventAppendEntriesResponse:
        // TODO add impl
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.LeaderUnsyncResponse{}
        e.SendResponse(ev.NewLeaderUnsyncResponseEvent(response))
        return nil
    }
    return self.Super()
}

func (self *UnsyncState) StartSync(raftHSM *RaftHSM) {
    // commit a blank no-op entry into the log at the start of leader's term

    term := raftHSM.GetCurrentTerm()
    leader, err := EncodeAddr(raftHSM.LocalAddr)
    if err != nil {
        // TODO add error handling
    }
    prevLogTerm, prevLogIndex := raftHSM.GetLastLogInfo()
    logEntry := &persist.LogEntry{
        Term:  term,
        Index: prevLogIndex + 1,
        Type:  persist.LogNoop,
        Data:  make([]byte, 0),
    }
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            leader,
        PrevLogTerm:       prevLogTerm,
        PrevLogIndex:      prevLogIndex,
        Entries:           []*persist.LogEntry{logEntry},
        LeaderCommitIndex: raftHSM.GetCommitIndex(),
    }
    event := ev.NewAppendEntriesRequestEvent(request)

    selfResponse := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: prevLogIndex,
        Success:      true,
    }
    raftHSM.SelfDispatch(ev.NewAppendEntriesResponseEvent(selfResponse))
    raftHSM.PeerManager.Broadcast(event)
}
