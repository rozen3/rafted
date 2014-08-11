package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "github.com/hhkbp2/rafted/persist"
    "net"
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
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    poolSize int,
    localAddr net.Addr,
    bindAddr net.Addr,
    otherPeerAddrs []net.Addr,
    configManager persist.ConfigManager,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.SnapshotManager,
    logger logging.Logger) (*RaftNode, error) {

    local := NewLocal(
        heartbeatTimeout,
        electionTimeout,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        logger)
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Dispatch(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Dispatch(event)
    }

    getLoggerForPeer := func(net.Addr) logging.Logger {
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
    server, err := comm.NewSocketServer(bindAddr, eventHandler2)
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
