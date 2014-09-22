package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
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
    config *Configuration,
    localAddr ps.ServerAddr,
    bindAddr ps.ServerAddr,
    otherPeerAddrs []ps.ServerAddr,
    configManager ps.ConfigManager,
    stateMachine ps.StateMachine,
    log ps.Log,
    logger logging.Logger) (*HSMBackend, error) {

    local, err := NewLocalManager(
        config,
        localAddr,
        log,
        stateMachine,
        configManager,
        logger)
    if err != nil {
        return nil, err
    }
    client := comm.NewSocketClient(config.CommPoolSize)
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
        config,
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
    server.Serve()
    return &HSMBackend{
        local:  local,
        peers:  peerManager,
        server: server,
    }, nil
}
