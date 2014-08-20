package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "testing"
    "time"
)

const (
    HeartbeatTimeout                        = time.Millisecond * 200
    ElectionTimeout                         = time.Millisecond * 50
    ElectionTimeoutThresholdPersent float32 = 0.8
    MaxAppendEntriesSize            uint64  = 10
    MaxSnapshotChunkSize            uint64  = 1000
    DefaultPoolSize                         = 2
)

func NewTestRaftNode(
    localAddr ps.ServerAddr,
    otherPeerAddrs []ps.ServerAddr,
    configManager ps.ConfigManager,
    stateMachine ps.StateMachine,
    log ps.Log,
    snapshotManager ps.SnapshotManager) (*RaftNode, error) {

    localLogger := logging.GetLogger(
        "leader" + "#" + localAddr.Network() + "://" + localAddr.String())
    local, err := NewLocal(
        HeartbeatTimeout,
        ElectionTimeout,
        ElectionTimeoutThresholdPersent,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        localLogger)
    if err != nil {
        // TODO error handling
        return nil, err
    }
    register := comm.NewMemoryTransportRegister()
    client := comm.NewMemoryClient(DefaultPoolSize, register)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Dispatch(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Dispatch(event)
    }
    getLoggerForPeer := func(peerAddr ps.ServerAddr) logging.Logger {
        return logging.GetLogger(
            "peer" + "#" + peerAddr.Network() + "://" + peerAddr.String())
    }
    peerManager := NewPeerManager(
        HeartbeatTimeout,
        MaxAppendEntriesSize,
        MaxSnapshotChunkSize,
        otherPeerAddrs,
        client,
        eventHandler1,
        local,
        getLoggerForPeer)
    serverLogger := logging.GetLogger(
        "Server" + "#" + localAddr.Network() + "://" + localAddr.String())
    server := comm.NewMemoryServer(
        &localAddr, eventHandler2, register, serverLogger)
    go func() {
        server.Serve()
    }()
    return &RaftNode{
        local:       local,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }, nil
}

func TestRafted(t *testing.T) {
    allAddrs := []ps.ServerAddr{
        ps.ServerAddr{
            Protocol: "memory",
            IP:       "127.0.0.1",
            Port:     6152,
        },
        ps.ServerAddr{
            Protocol: "memory",
            IP:       "127.0.0.1",
            Port:     6153,
        },
    }
    stateMachine := ps.NewMemoryStateMachine()
    log := ps.NewMemoryLog()
    snapshotManager := ps.NewMemorySnapshotManager()
    firstLogIndex, _ := log.FirstIndex()
    config := &ps.Config{
        Servers:    allAddrs,
        NewServers: nil,
    }
    configManager := ps.NewMemoryConfigManager(firstLogIndex, config)
    node1, err := NewTestRaftNode(allAddrs[0], allAddrs[1:],
        configManager, stateMachine, log, snapshotManager)
    if err != nil {
        t.Error("fail to create test raft node, error:", err)
    }
    t.Log(node1)
    // node2 := NewTestRaftNode(allAddrs[1], allAddrs[:1],
    //     configManager, stateMachine, log, snapshotManager)
    // t.Log(node2)
}
