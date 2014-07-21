package state

import "fmt"
import hsm "github.com/hhkbp2/go-hsm"
import rafted "github.com/hhkbp2/rafted"

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
    case IsRaftRequest(event.Type()):
        e, ok := event.(RaftEvent)
        hsm.AssertTrue(ok)
        response, err := peerHSM.Client.CallRPCTo(peerHSM.Addr, e)
        if err != nil {
            // TODO add log
            return nil
        }
        peerHSM.SelfDispatch(response)
        return nil
    case event.Type() == EventRequestVoteResponse:
        fallthrough
    case event.Type() == EventAppendEntriesResponse:
        fallthrough
    case event.Type() == EventInstallSnapshotResponse:
        e, ok := event.(RaftEvent)
        hsm.AssertTrue(ok)
        peerHSM.EventHandler(e)
        return nil
    }
    // ignore all other events
    return self.Super()
}
