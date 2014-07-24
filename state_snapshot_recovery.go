package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
)

type SnapshotRecoveryState struct {
    *hsm.StateHead

    // TODO add fields
}

func NewSnapshotRecoveryState(super hsm.State) *SnapshotRecoveryState {
    object := &SnapshotRecoveryState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*SnapshotRecoveryState) ID() string {
    return StateSnapshotRecoveryID
}

func (self *SnapshotRecoveryState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    return self.Super()
}

func (self *SnapshotRecoveryState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    return self.Super()
}

func (self *SnapshotRecoveryState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event=", event)
    _, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutHeartBeat:
        // Ignore this event. Don't transfer to candidate state when
        // recovering from snapshot.
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        // TODO
        return nil
    }
    return self.Super()
}
