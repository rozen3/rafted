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

func (self *PeerState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerIdleID)
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
        e, ok := event.(ev.RaftEvent)
        hsm.AssertTrue(ok)
        peerHSM.EventHandler(e)
        return nil
    case event.Type() == ev.EventAppendEntriesResponse:
        // TODO
        return nil
    case event.Type() == ev.EventInstallSnapshotResponse:
        // TODO
        return nil
    }
    // ignore all other events
    return self.Super()
}

type PeerIdleState struct {
    *hsm.StateHead
}

func NewPeerIdleState(super hsm.State) *PeerIdleState {
    object := &PeerIdleState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerIdleState) ID() string {
    return StatePeerIdleID
}

func (self *PeerIdleState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerIdleState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    return self.Super()
}

func (self *PeerIdleState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerIdleState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    return self.Super()
}

type PeerReplicatingState struct {
    *hsm.StateHead
}

func NewPeerReplicatingState(super hsm.State) *PeerReplicatingState {
    object := &PeerReplicatingState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerReplicatingState) ID() string {
    return StatePeerReplicatingID
}

func (self *PeerReplicatingState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    // TODO add impl
    return nil
}

func (self *PeerReplicatingState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    sm.QInit(StatePeerStandardModeID)
    return nil
}

func (self *PeerReplicatingState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerReplicatingState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    return self.Super()
}

type PeerStandardModeState struct {
    *hsm.StateHead
}

func NewPeerStandardModeState(super hsm.State) *PeerStandardModeState {
    object := &PeerStandardModeState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerStandardModeState) ID() string {
    return StatePeerStandardModeID
}

func (self *PeerStandardModeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil

}

func (self *PeerReplicatingState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Init")
    return self.Super()
}

func (self *PeerReplicatingState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerReplicatingState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    return self.Super()
}

type PeerPipelineModeState struct {
    *hsm.StateHead
}

func NewPeerPipelineModeState(super hsm.State) *PeerPipelineModeState {
    object := &PeerPipelineModeState{
        StateHead: hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*PeerPipelineModeState) ID() string {
    return StatePeerPipelineModeID
}

func (self *PeerPipelineModeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *PeerPipelineModeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *PeerPipelineModeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle")
    // TODO add impl
    return self.Super()
}
