package rafted

import "time"
import hsm "github.com/hhkbp2/go-hsm"

const (
    FollowerID  = "follower"
    CandidateID = "candidate"
    LeaderID    = "leader"
)

func CreateRaftHSM(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration) TerminableHSM {
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, FollowerID)
    NewFollower(top, heartbeatTimeout)
    NewCandidate(top, electionTimeout)
    NewLeader(top)
    raftHSM := NewRaftHSM(top, initial)
    raftHSM.Init()
    return raftHSM
}
