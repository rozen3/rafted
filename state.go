package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    logging "github.com/hhkbp2/rafted/logging"
)

const (
    StateLocalID                         = "local"
    StateFollowerID                      = "follower"
    StateSnapshotRecoveryID              = "snapshot_recovery"
    StateFollowerMemberChangeID          = "follower_member_change"
    StateFollowerOldNewConfigSeenID      = "follower_old_new_config_seen"
    StateFollowerOldNewConfigCommittedID = "follower_old_new_config_committed"
    StateFollowerNewConfigSeenID         = "follower_new_config_seen"
    StateNeedPeersID                     = "need_peers"
    StateCandidateID                     = "candidate"
    StateLeaderID                        = "leader"
    StateUnsyncID                        = "unsync"
    StateSyncID                          = "sync"
    StatePersistErrorID                  = "persist_error"
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

const (
    StateLeaderMemberChangeID            = "leader_member_change"
    StateLeaderMemberChangeDeactivatedID = "leader_member_change_deactivated"
    StateLeaderMemberChangeActivatedID   = "leader_member_change_activated"
    StateLeaderNotInMemberChangeID       = "leader_not_in_member_change"
    StateLeaderInMemberChangeID          = "leader_in_member_change"
    StateLeaderMemberChangePhase1ID      = "leader_member_change_phase1"
    StateLeaderMemberChangePhase2ID      = "leader_member_change_phase2"
)

type LogState interface {
    hsm.State
    logging.Logger
}

type LogStateHead struct {
    *hsm.StateHead
    logging.Logger
}

func NewLogStateHead(super hsm.State, logger logging.Logger) *LogStateHead {
    return &LogStateHead{
        StateHead: hsm.NewStateHead(super),
        Logger:    logger,
    }
}
