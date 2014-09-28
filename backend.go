package rafted

import (
    "github.com/zonas/rafted/comm"
    ev "github.com/zonas/rafted/event"
    logging "github.com/zonas/rafted/logging"
    ps "github.com/zonas/rafted/persist"
    "io"
)

type Notifiable interface {
    GetNotifyChan() <-chan ev.NotifyEvent
}

type Backend interface {
    Send(event ev.RequestEvent)
    io.Closer
    Notifiable
}

type HSMBackend struct {
    local  Local
    peers  Peers
    server comm.Server
}

func (self *HSMBackend) Send(event ev.RequestEvent) {
    self.local.Send(event)
}

func (self *HSMBackend) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.local.Notifier().GetNotifyChan()
}

func (self *HSMBackend) Close() error {
    self.server.Close()
    self.peers.Close()
    self.local.Close()
    return nil
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
    client := comm.NewSocketClient(config.CommPoolSize, config.CommClientTimeout)
    getLoggerForPeer := func(ps.ServerAddr) logging.Logger {
        return logger
    }
    peerManager := NewPeerManager(
        config,
        otherPeerAddrs,
        client,
        local,
        getLoggerForPeer,
        logger)
    eventHandler := func(event ev.RequestEvent) {
        local.Send(event)
    }
    server, err := comm.NewSocketServer(
        &bindAddr, config.CommServerTimeout, eventHandler, logger)
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
