package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "testing"
    "time"
)

const (
    HeartbeatTimeout = time.Millisecond * 200
    ElectionTimeout  = time.Millisecond * 50
    DefaultPoolSize  = 2
)

func NewTestRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr,
    peerAddrs []net.Addr) *RaftNode {

    raftHSM := CreateRaftHSM(heartbeatTimeout, electionTimeout, localAddr)

    register := comm.NewMemoryTransportRegister()
    client := comm.NewMemoryClient(DefaultPoolSize, register)
    eventHandler1 := func(event ev.RaftEvent) {
        raftHSM.Dispatch(event)
    }
    eventHandler2 := func(event ev.RequestEvent) {
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
        comm.NewMemoryAddr(),
        comm.NewMemoryAddr(),
    }
    node1 := NewTestRaftNode(HeartbeatTimeout, ElectionTimeout, allAddrs[0], allAddrs[1:])
    //    go node1.Run()
    t.Log(node1)
    node2 := NewTestRaftNode(HeartbeatTimeout, ElectionTimeout, allAddrs[1], allAddrs[:1])
    //    go node2.Run()
    t.Log(node2)
}
