package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

type Notifiable interface {
    GetNotifyChan() <-chan ev.NotifyEvent
}

type Backend interface {
    Send(event ev.RaftRequestEvent)
    Close()
    Notifiable
}

type HSMBackend struct {
    local  Local
    peers  Peers
    server comm.Server
}

func (self *HSMBackend) Send(event ev.RaftRequestEvent) {
    self.local.Send(event)
}

func (self *HSMBackend) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.local.Notifier().GetNotifyChan()
}

func (self *HSMBackend) Close() {
    self.local.Close()
    self.peers.Close()
    self.server.Close()
}

func NewHSMBackend(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    electionTimeoutThresholdPersent float64,
    maxTimeoutJitter float32,
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

    local, err := NewLocalManager(
        heartbeatTimeout,
        electionTimeout,
        electionTimeoutThresholdPersent,
        maxTimeoutJitter,
        persistErrorNotifyTimeout,
        localAddr,
        log,
        stateMachine,
        snapshotManager,
        configManager,
        logger)
    if err != nil {
        return nil, err
    }
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Send(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Send(event)
    }

    getLoggerForPeer := func(ps.ServerAddr) logging.Logger {
        return logger
    }
    peerManager := NewPeerManager(
        heartbeatTimeout,
        maxTimeoutJitter,
        maxAppendEntriesSize,
        maxSnapshotChunkSize,
        otherPeerAddrs,
        client,
        eventHandler1,
        local,
        getLoggerForPeer,
        logger)
    server, err := comm.NewSocketServer(&bindAddr, eventHandler2, logger)
    if err != nil {
        // TODO add cleanup
        return nil, err
    }
    go func() {
        server.Serve()
    }()
    return &HSMBackend{
        local:  local,
        peers:  peerManager,
        server: server,
    }, nil
}
