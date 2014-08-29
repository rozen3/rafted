package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/stretchr/testify/assert"
    "testing"
    "time"
)

const (
    HeartbeatTimeout                        = time.Millisecond * 200
    ElectionTimeout                         = time.Millisecond * 50
    ElectionTimeoutThresholdPersent float64 = 0.8
    PersistErrorNotifyTimeout               = time.Millisecond * 100
    MaxAppendEntriesSize            uint64  = 10
    MaxSnapshotChunkSize            uint64  = 1000
    DefaultPoolSize                         = 10
)

func RemoveAddr(addrs []ps.ServerAddr, addr ps.ServerAddr) []ps.ServerAddr {
    result := make([]ps.ServerAddr, 0, len(addrs))
    for _, a := range addrs {
        if ps.AddrNotEqual(&a, &addr) {
            result = append(result, addr)
        }
    }
    return result
}

func NewTestHSMBackend(
    localAddr ps.ServerAddr, addrs []ps.ServerAddr) (*HSMBackend, error) {

    log := ps.NewMemoryLog()
    firstLogIndex, _ := log.FirstIndex()
    config := &ps.Config{
        Servers:    addrs,
        NewServers: nil,
    }
    configManager := ps.NewMemoryConfigManager(firstLogIndex, config)
    stateMachine := ps.NewMemoryStateMachine()
    snapshotManager := ps.NewMemorySnapshotManager()

    localLogger := logging.GetLogger(
        "leader" + "#" + localAddr.Network() + "://" + localAddr.String())
    local, err := NewLocal(
        HeartbeatTimeout,
        ElectionTimeout,
        ElectionTimeoutThresholdPersent,
        PersistErrorNotifyTimeout,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        localLogger)
    if err != nil {
        return nil, err
    }
    client := comm.NewMemoryClient(DefaultPoolSize, testRegister)
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
        RemoveAddr(addrs, localAddr),
        client,
        eventHandler1,
        local,
        getLoggerForPeer)
    serverLogger := logging.GetLogger(
        "Server" + "#" + localAddr.Network() + "://" + localAddr.String())
    server := comm.NewMemoryServer(
        &localAddr, eventHandler2, testRegister, serverLogger)
    go func() {
        server.Serve()
    }()
    return &HSMBackend{
        local:       local,
        peerManager: peerManager,
        server:      server,
    }, nil
}

func TestHSMBackend(t *testing.T) {
    // TODO add impl
    assert.Equal(t, true, true)
}