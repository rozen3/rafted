package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

type RaftNode struct {
    local       *Local
    peerManager *PeerManager
    client      comm.Client
    server      comm.Server
}

// TODO add logger
func NewRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    electionTimeoutThresholdPersent float32,
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    poolSize int,
    localAddr ps.ServerAddr,
    bindAddr ps.ServerAddr,
    otherPeerAddrs []ps.ServerAddr,
    configManager ps.ConfigManager,
    stateMachine ps.StateMachine,
    log ps.Log,
    snapshotManager ps.SnapshotManager,
    logger logging.Logger) (*RaftNode, error) {

    local, err := NewLocal(
        heartbeatTimeout,
        electionTimeout,
        electionTimeoutThresholdPersent,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        logger)
    if err != nil {
        return nil, err
    }
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Dispatch(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Dispatch(event)
    }

    getLoggerForPeer := func(ps.ServerAddr) logging.Logger {
        return logger
    }
    peerManager := NewPeerManager(
        heartbeatTimeout,
        maxAppendEntriesSize,
        maxSnapshotChunkSize,
        otherPeerAddrs,
        client,
        eventHandler1,
        local,
        getLoggerForPeer)
    server, err := comm.NewSocketServer(&bindAddr, eventHandler2)
    if err != nil {
        // TODO add cleanup
        return nil, err
    }
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
