package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    "testing"
)

const (
    HeartbeatTimeout = time.Millisecond * 200
    ElectionTimeout  = time.Millisecond * 50
)

func NewTestRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr,
    peerAddrs []net.Addr) *RaftNode {

    raftHSM := NewRaftHSM(heartbeatTimeout, electionTimeout, localAddr)

    register := comm.NewMemoryTransportRegister()
    client := comm.NewMemoryClient(localAddr, register)
    eventHandler1 := func(event RaftEvent) {
        raftHSM.Dispatch(event)
    }
    eventHandler2 := func(event RequestEvent) {
        raftHSM.Dispatch(event)
    }
    peerManager := NewPeerManager(peerAddrs, client, eventHandler1)
    raftHSM.SetPeerManager(peerManager)
    server := comm.NewMemoryServer(eventHandler2, localAddr, register)
    return &RaftNode{
        raftHSM:     raftHSM,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }
}

func TestRafted(t *testing.T) {
    allAddrs := []net.Addr{
        NewMemoryAddr(),
        NewMemoryAddr(),
    }
    node1 := NewTestRaftNode(HeartbeatTimeout, ElectionTimeout, allAddrs[0], allAddrs[1:])
    //    go node1.Run()
    node2 := NewTestRaftNode(HeartbeatTimeout, ElectionTimeout, allAddrs[1], allAddrs[:1])
    //    go node2.Run()
}
