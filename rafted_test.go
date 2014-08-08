package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "github.com/hhkbp2/rafted/persist"
    "net"
    "testing"
    "time"
)

const (
    HeartbeatTimeout            = time.Millisecond * 200
    ElectionTimeout             = time.Millisecond * 50
    MaxAppendEntriesSize uint64 = 10
    MaxSnapshotChunkSize uint64 = 1000
    DefaultPoolSize             = 2
)

func NewTestRaftNode(
    localAddr net.Addr,
    otherPeerAddrs []net.Addr,
    configManager persist.ConfigManager,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.SnapshotManager) *RaftNode {

    local := NewLocal(
        HeartbeatTimeout,
        ElectionTimeout,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager)
    register := comm.NewMemoryTransportRegister()
    client := comm.NewMemoryClient(DefaultPoolSize, register)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Dispatch(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Dispatch(event)
    }
    peerManager := NewPeerManager(
        HeartbeatTimeout,
        MaxAppendEntriesSize,
        MaxSnapshotChunkSize,
        otherPeerAddrs,
        client,
        eventHandler1,
        local)
    server := comm.NewMemoryServer(localAddr, eventHandler2, register)
    go func() {
        server.Serve()
    }()
    return &RaftNode{
        local:       local,
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
    stateMachine := persist.NewMemoryStateMachine()
    log := persist.NewMemoryLog()
    snapshotManager := persist.NewMemorySnapshotManager()
    firstLogIndex, _ := log.FirstIndex()
    config := &persist.Config{
        Servers:    allAddrs,
        NewServers: nil,
    }
    configManager := persist.NewMemoryConfigManager(firstLogIndex, config)
    node1 := NewTestRaftNode(allAddrs[0], allAddrs[1:],
        configManager, stateMachine, log, snapshotManager)
    t.Log(node1)
    // node2 := NewTestRaftNode(allAddrs[1], allAddrs[:1],
    //     configManager, stateMachine, log, snapshotManager)
    // t.Log(node2)
}
