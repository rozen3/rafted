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

    localLogger := logging.GetLogger("leader" + "#" + localAddr.String())
    local, err := NewLocalManager(
        testConfig,
        localAddr,
        log,
        stateMachine,
        configManager,
        localLogger)
    if err != nil {
        return nil, err
    }
    client := comm.NewMemoryClient(
        testConfig.CommPoolSize, testConfig.CommTimeout, testRegister)
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
        testConfig,
        RemoveAddr(addrs, localAddr),
        client,
        eventHandler1,
        local,
        getLoggerForPeer,
        peerManagerLogger)
    serverLogger := logging.GetLogger("Server" + "#" + localAddr.String())
    server := comm.NewMemoryServer(
        &localAddr,
        testConfig.CommTimeout,
        eventHandler2,
        testRegister,
        serverLogger)
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
    time.Sleep(testConfig.ElectionTimeout)
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
    time.Sleep(testConfig.ElectionTimeout)

    request := &ev.ClientAppendRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    i := 0
Outermost:
    for {
        backends[i].Send(reqEvent)
        event := reqEvent.RecvResponse()
        switch event.Type() {
        case ev.EventClientResponse:
            e, ok := event.(*ev.ClientResponseEvent)
            assert.True(t, ok)
            response := e.Response
            assert.Equal(t, true, response.Success)
            assert.Equal(t, testData, response.Data)
            break Outermost
        default:
            time.Sleep(testConfig.HeartbeatTimeout)
        }
        i = (i + 1) % clusterSize
    }
    // cleanup
    for _, backend := range backends {
        backend.Close()
    }
}
