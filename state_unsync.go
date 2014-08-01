package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    persist "github.com/hhkbp2/rafted/persist"
)

type UnsyncState struct {
    *hsm.StateHead

    noop     *InflightRequest
    listener *ClientEventListener
}

func NewUnsyncState(super hsm.State) *UnsyncState {
    ch := make(chan ev.ClientEvent, 1)
    object := &UnsyncState{
        StateHead: hsm.NewStateHead(super),
        noop: &InflightRequest{
            LogType:    persist.LogNoop,
            Data:       make([]byte, 0),
            ResultChan: ch,
        },
        listener: NewClientEventListener(ch),
    }
    super.AddChild(object)
    return object
}

func (*UnsyncState) ID() string {
    return StateUnsyncID
}

func (self *UnsyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    handleNoopResponse := func(event ev.ClientEvent) {
        if event.Type() != ev.EventClientResponse {
            // TODO add log
            return
        }
        raftHSM.SelfDispatch(event)
    }
    self.listener.Start(handleNoopResponse)
    self.StartSyncSafe(raftHSM)
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
    self.listener.Stop()
    return nil
}

func (self *UnsyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.ClientRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.LeaderUnsyncResponse{}
        e.SendResponse(ev.NewLeaderUnsyncResponseEvent(response))
        return nil
    case event.Type() == ev.EventClientResponse:
        e, ok := event.(*ev.ClientResponseEvent)
        hsm.AssertTrue(ok)
        // TODO add different policy for retry
        if e.Response.Success {
            raftHSM.QTran(StateSyncID)
        } else {
            self.StartSyncSafe(raftHSM)
        }
        return nil
    }
    return self.Super()
}

func (self *UnsyncState) StartSync(raftHSM *RaftHSM) error {
    // commit a blank no-op entry into the log at the start of leader's term
    requests := []*InflightRequest{self.noop}
    leaderState, ok := self.Super().(*LeaderState)
    hsm.AssertTrue(ok)
    return leaderState.StartFlight(raftHSM, requests)
}

func (self *UnsyncState) StartSyncSafe(raftHSM *RaftHSM) {
    if err := self.StartSync(raftHSM); err != nil {
        // TODO add log
        stepdown := &ev.Stepdown{}
        raftHSM.SelfDispatch(ev.NewStepdownEvent(stepdown))
    }
}
