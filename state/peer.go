package state

import "fmt"
import hsm "github.com/hhkbp2/go-hsm"
import rafted "github.com/hhkbp2/rafted"

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
    return nil
}

func (self *PeerState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event =", event)
    peerHSM, ok := sm.(*PeerHSM)
    hsm.AssertTrue(ok)
    switch {
    case IsRaftEvent(event.Type()):
        response, err := peerHSM.Client.SendRecv
    }
    // ignore all other events
    return nil
}

func (self *)
