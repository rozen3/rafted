package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "time"
)

func CreateRaftHSM(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr) *RaftHSM {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateRaftID)
    raftState := NewRaftState(top)
    NewFollowerState(raftState, heartbeatTimeout)
    NewCandidateState(raftState, electionTimeout)
    NewLeaderState(raftState)
    raftHSM := NewRaftHSM(top, initial, localAddr)
    raftHSM.Init()
    return raftHSM
}

type RaftNode struct {
    raftHSM     *RaftHSM
    peerManager *PeerManager
    client      comm.Client
    server      comm.Server
}

func NewRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    poolSize int,
    localAddr net.Addr,
    bindAddr net.Addr,
    peerAddrs []net.Addr) (*RaftNode, error) {

    raftHSM := CreateRaftHSM(heartbeatTimeout, electionTimeout, localAddr)
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event ev.RaftEvent) {
        raftHSM.Dispatch(event)
    }
    eventHandler2 := func(event ev.RequestEvent) {
        raftHSM.Dispatch(event)
    }
    peerManager := NewPeerManager(peerAddrs, client, eventHandler1)
    raftHSM.SetPeerManager(peerManager)
    server, err := comm.NewSocketServer(bindAddr, eventHandler2)
    if err != nil {
        // TODO add cleanup
        return nil, err
    }
    return &RaftNode{
        raftHSM:     raftHSM,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }, nil
}

func (self *RaftNode) Run() {
    self.server.Serve()
}
