package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
)

const (
    StateLocalID            = "local"
    StateFollowerID         = "follower"
    StateSnapshotRecoveryID = "snapshot_recovery"
    StateNeedPeersID        = "need_peers"
    StateCandidateID        = "candidate"
    StateLeaderID           = "leader"
    StateUnsyncID           = "unsync"
    StateSyncID             = "sync"
)

const (
    StatePeerID             = "peer"
    StateDeactivatedPeerID  = "deactivated_peer"
    StateActivatedPeerID    = "activated_peer"
    StateCandidatePeerID    = "candidate_peer"
    StateLeaderPeerID       = "leader_peer"
    StateStandardModePeerID = "standard_mode_peer"
    StateSnapshotModePeerID = "snapshot_mode_peer"
    StatePipelineModePeerID = "pipeline_mode_peer"
)

type LocalState struct {
    *hsm.StateHead
}

func NewLocalState(super hsm.State) *LocalState {
    object := &LocalState{
        hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*LocalState) ID() string {
    return StateLocalID
}

func (self *LocalState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // ignore events
    return nil
}

func (self *LocalState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    sm.QInit(StateFollowerID)
    return nil
}

func (self *LocalState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // ignore events
    return nil
}

func (self *LocalState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    // TODO add event handling if needed
    return nil
}

type NeedPeersState struct {
    *hsm.StateHead
}

func NewNeedPeersState(super hsm.State) *NeedPeersState {
    object := &NeedPeersState{
        hsm.NewStateHead(super),
    }
    super.AddChild(object)
    return object
}

func (*NeedPeersState) ID() string {
    return StateNeedPeersID
}

func (self *NeedPeersState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // coordinate peer into ActivatedPeerState
    localHSM.PeerManager().Broadcast(ev.NewPeerActivateEvent())
    return nil
}

func (self *NeedPeersState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // coordinate peer into DeactivatePeerState
    localHSM.PeerManager().Broadcast(ev.NewPeerDeactivateEvent())
    return nil
}

func (self *NeedPeersState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    // TODO add log
    return self.Super()
}
