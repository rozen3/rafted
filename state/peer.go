package state

import "fmt"

type PeerState struct {
    hsm.StateHead
}

func NewPeerState(super hsm.State) *PeerState {
    object := &PeerState{
        StateHead: hsm.MakeStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerState) ID() string {
    return PeerStateID
}

func (self *PeerState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    return nil
}

func (self *PeerState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    // TODO add impl
    return nil
}

func (self *PeerState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle")
    // TODO add impl
    return nil
}
