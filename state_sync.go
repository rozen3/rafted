package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
)

type SyncState struct {
    *hsm.StateHead

    // TODO add fields
}

func NewSyncState(super hsm.State) *SyncState {
    object := &SyncState{
        hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*SyncState) ID() string {
    return StateSyncID
}

func (self *SyncState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    return nil
}

func (self *SyncState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // TODO add impl
    return nil
}

func (self *SyncState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    // TODO add impl
    return self.Super()
}
