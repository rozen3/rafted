package rafted

import (
    "github.com/hhkbp2/rafted/comm"
)

func CreateRaftHSM(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr) *RaftHSM {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, FollowerID)
    raftState := NewRaftState(top)
    NewFollower(raftState, heartbeatTimeout)
    NewCandidate(raftState, electionTimeout)
    NewLeader(raftState, top)
    raftHSM := NewRaftHSM(top, initial, localAddr)
    raftHSM.Init()
    return raftHSM
}

type RaftNode struct {
    raftHSM     *RaftHSM
    peerManager *PeerManager
    client      *comm.Client
    server      *comm.Server
}

func NewRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    poolSize uint32,
    localAddr net.Addr,
    bindAddr net.Addr,
    peerAddrs []net.Addr) *RaftNode {

    raftHSM := NewRaftHSM(heartbeatTimeout, electionTimeout, localAddr)
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event RaftEvent) {
        raftHSM.Dispatch(event)
    }
    eventHandler2 := func(event RequestEvent) {
        raftHSM.Dispatch(event)
    }
    peerManager := NewPeerManager(peerAddrs, client, eventHandler1)
    raftHSM.SetPeerManager(peerManager)
    server := comm.NewSocketServer(bindAddr, eventHandler2)
    return &RaftNode{
        raftHSM:     raftHSM,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }
}

func (self *RaftNode) Run() {
    self.server.Serve()
}
