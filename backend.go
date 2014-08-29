package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

type Backend interface {
    Send(event ev.RaftRequestEvent)
}

type HSMBackend struct {
    local       *Local
    peerManager *PeerManager
    server      comm.Server
}

func (self *HSMBackend) Send(event ev.RaftRequestEvent) {
    self.local.Dispatch(event)
}

func NewHSMBackend(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    electionTimeoutThresholdPersent float64,
    persistErrorNotifyTimeout time.Duration,
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
    logger logging.Logger) (*HSMBackend, error) {

    local, err := NewLocal(
        heartbeatTimeout,
        electionTimeout,
        electionTimeoutThresholdPersent,
        persistErrorNotifyTimeout,
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
    server, err := comm.NewSocketServer(&bindAddr, eventHandler2, logger)
    if err != nil {
        // TODO add cleanup
        return nil, err
    }
    go func() {
        server.Serve()
    }()
    return &HSMBackend{
        local:       local,
        peerManager: peerManager,
        server:      server,
    }, nil
}
