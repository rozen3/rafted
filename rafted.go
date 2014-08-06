package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "time"
)

type RaftNode struct {
    local       *Local
    peerManager *PeerManager
    client      comm.Client
    server      comm.Server
}

func NewRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    poolSize int,
    localAddr net.Addr,
    bindAddr net.Addr,
    configManager persist.ConfigManager,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.snapshotManager) (*RaftNode, error) {

    local := NewLocal(
        heartbeatTimeout,
        electionTimeout,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager)
    client := comm.NewSocketClient(poolSize)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Dispatch(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Dispatch(event)
    }
    peerManager := NewPeerManager(local, otherPeerAddrs, client, eventHandler1)
    local.SetPeerManager(peerManager)
    server, err := comm.NewSocketServer(bindAddr, eventHandler2)
    if err != nil {
        // TODO add cleanup
        return nil, err
    }
    go func() {
        self.server.Serve()
    }()
    return &RaftNode{
        local:       local,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }, nil
}
