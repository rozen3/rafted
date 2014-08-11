package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    logging "github.com/hhkbp2/rafted/logging"
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

type LogState interface {
    hsm.State
    Log() logging.Logger
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
