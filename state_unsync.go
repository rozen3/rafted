package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
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
    return self.Super()
}
