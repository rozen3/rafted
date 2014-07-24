package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
)

const (
    StateRaftID             = "raft"
    StateFollowerID         = "follower"
    StateSnapshotRecoveryID = "snapshot_recovery"
    StateCandidateID        = "candidate"
    StateLeaderID           = "leader"
)

const (
    StatePeerID = "peer"
)

type RaftState struct {
    *hsm.StateHead
}

func NewRaftState(super hsm.State) *RaftState {
    object := &RaftState{
        hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*RaftState) ID() string {
    return StateRaftID
}

func (self *RaftState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // ignore events
    return nil
}

func (self *RaftState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    sm.QInit(StateFollowerID)
    return nil
}

func (self *RaftState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // ignore events
    return nil
}

func (self *RaftState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // TODO add event handling if needed
    return nil
}
