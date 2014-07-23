package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
)

type PeerState struct {
    *hsm.StateHead
}

func NewPeerState(super hsm.State) *PeerState {
    object := &PeerState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerState) ID() string {
    return StatePeerID
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
    case ev.IsRaftRequest(event.Type()):
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        response, err := peerHSM.Client.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO add log
            return nil
        }
        peerHSM.SelfDispatch(response)
        return nil
    case event.Type() == ev.EventRequestVoteResponse:
        fallthrough
    case event.Type() == ev.EventAppendEntriesResponse:
        fallthrough
    case event.Type() == ev.EventInstallSnapshotResponse:
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerHSM.EventHandler(e)
        return nil
    }
    // ignore all other events
    return self.Super()
}
