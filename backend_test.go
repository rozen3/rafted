package rafted

import (
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
    "time"
)

const (
    HeartbeatTimeout                        = time.Millisecond * 50
    ElectionTimeout                         = time.Millisecond * 200
    ElectionTimeoutThresholdPersent float64 = 0.8
    MaxTimeoutJitter                float32 = 0.1
    PersistErrorNotifyTimeout               = time.Millisecond * 100
    MaxAppendEntriesSize            uint64  = 10
    MaxSnapshotChunkSize            uint64  = 1000
    CommTimeout                             = HeartbeatTimeout
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

    localLogger := logging.GetLogger("leader" + "#" + localAddr.String())
    local, err := NewLocalManager(
        HeartbeatTimeout,
        ElectionTimeout,
        ElectionTimeoutThresholdPersent,
        MaxTimeoutJitter,
        PersistErrorNotifyTimeout,
        localAddr,
        log,
        stateMachine,
        snapshotManager,
        configManager,
        localLogger)
    if err != nil {
        return nil, err
    }
    client := comm.NewMemoryClient(DefaultPoolSize, CommTimeout, testRegister)
    eventHandler1 := func(event ev.RaftEvent) {
        local.Send(event)
    }
    eventHandler2 := func(event ev.RaftRequestEvent) {
        local.Send(event)
    }
    getLoggerForPeer := func(peerAddr ps.ServerAddr) logging.Logger {
        return logging.GetLogger(
            "peer" + "#" + localAddr.String() + ">>" + peerAddr.String())
    }
    peerManagerLogger := logging.GetLogger(
        "peer manager" + "#" + localAddr.String())
    peerManager := NewPeerManager(
        HeartbeatTimeout,
        MaxTimeoutJitter,
        MaxAppendEntriesSize,
        MaxSnapshotChunkSize,
        RemoveAddr(addrs, localAddr),
        client,
        eventHandler1,
        local,
        getLoggerForPeer,
        peerManagerLogger)
    serverLogger := logging.GetLogger("Server" + "#" + localAddr.String())
    server := comm.NewMemoryServer(
        &localAddr, CommTimeout, eventHandler2, testRegister, serverLogger)
    go func() {
        server.Serve()
    }()
    return &HSMBackend{
        local:  local,
        peers:  peerManager,
        server: server,
    }, nil
}

func TestBackendOneNodeCluster(t *testing.T) {
    servers := testServers[0:1]
    serverAddr := servers[0]
    backend, err := NewTestHSMBackend(serverAddr, servers)
    assert.Nil(t, err)
    assert.NotNil(t, backend)
    time.Sleep(ElectionTimeout)
    request := &ev.ClientAppendRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    backend.Send(reqEvent)
    assertGetClientResponseEvent(t, reqEvent, true, testData)
}

func TestBackendContruction(t *testing.T) {
    servers := testServers
    clusterSize := 3
    backends := make([]Backend, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := NewTestHSMBackend(servers[i], servers)
        assert.Nil(t, err)
        backends = append(backends, backend)
    }
    time.Sleep(ElectionTimeout)

    request := &ev.ClientAppendRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    for i := 0; i < len(backends); i++ {
        backends[i].Send(reqEvent)
        event := reqEvent.RecvResponse()
        switch event.Type() {
        case ev.EventLeaderRedirectResponse:
            continue
        case ev.EventClientResponse:
            e, ok := event.(*ev.ClientResponseEvent)
            assert.True(t, ok)
            response := e.Response
            assert.Equal(t, true, response.Success)
            assert.Equal(t, testData, response.Data)
            break
        }
    }
    // cleanup
    for _, backend := range backends {
        backend.Close()
    }
}
