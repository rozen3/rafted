package rafted

import (
    "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
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

type GenHSMBackendFunc func(
    localAddr ps.ServerAddr, addrs []ps.ServerAddr) (*HSMBackend, error)

type GenServerFunc func(
    handler cm.RequestEventHandler, logger logging.Logger) (cm.Server, error)

func NewTestBackendWith(
    localAddr ps.ServerAddr,
    addrs []ps.ServerAddr,
    client cm.Client,
    genServer GenServerFunc) (*HSMBackend, error) {

    log := ps.NewMemoryLog()
    firstLogIndex, err := log.FirstIndex()
    if err != nil {
        return nil, err
    }
    config := &ps.Config{
        Servers:    addrs,
        NewServers: nil,
    }
    configManager := ps.NewMemoryConfigManager(firstLogIndex, config)
    stateMachine := ps.NewMemoryStateMachine()

    localLogger := logging.GetLogger("local" + "#" + localAddr.String())
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
        local,
        getLoggerForPeer,
        peerManagerLogger)

    eventHandler := func(event ev.RequestEvent) {
        local.Send(event)
    }
    serverLogger := logging.GetLogger("Server" + "#" + localAddr.String())
    server, err := genServer(eventHandler, serverLogger)
    if err != nil {
        return nil, err
    }
    server.Serve()
    object := &HSMBackend{
        local:  local,
        peers:  peerManager,
        server: server,
    }
    return object, nil
}

func NewTestMemoryHSMBackend(
    localAddr ps.ServerAddr, addrs []ps.ServerAddr) (*HSMBackend, error) {

    client := cm.NewMemoryClient(
        testConfig.CommPoolSize, testConfig.CommClientTimeout, testRegister)
    genServer := func(
        handler cm.RequestEventHandler,
        logger logging.Logger) (cm.Server, error) {

        server := cm.NewMemoryServer(
            &localAddr,
            testConfig.CommServerTimeout,
            handler,
            testRegister,
            logger)
        return server, nil
    }
    return NewTestBackendWith(localAddr, addrs, client, genServer)
}

func NewTestSocketHSMBackend(
    localAddr ps.ServerAddr, addrs []ps.ServerAddr) (*HSMBackend, error) {

    client := cm.NewSocketClient(
        testConfig.CommPoolSize, testConfig.CommClientTimeout)
    genServer := func(
        handler cm.RequestEventHandler,
        logger logging.Logger) (cm.Server, error) {

        server, err := cm.NewSocketServer(
            &localAddr,
            testConfig.CommServerTimeout,
            handler,
            logger)
        return server, err
    }
    return NewTestBackendWith(localAddr, addrs, client, genServer)
}

func NewTestRPCHSMBackend(
    localAddr ps.ServerAddr, addrs []ps.ServerAddr) (*HSMBackend, error) {

    client := cm.NewRPCClient(testConfig.CommClientTimeout)
    genServer := func(
        handler cm.RequestEventHandler,
        logger logging.Logger) (cm.Server, error) {

        return cm.NewRPCServer(
            &localAddr,
            testConfig.CommServerTimeout,
            handler,
            logger)
    }
    return NewTestBackendWith(localAddr, addrs, client, genServer)
}

func TestMemoryBackendOneNodeCluster(t *testing.T) {
    servers := testServers[0:1]
    serverAddr := servers[0]
    backend, err := NewTestMemoryHSMBackend(serverAddr, servers)
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

func testBackendConstruction(
    t *testing.T, servers []ps.ServerAddr, genBackend GenHSMBackendFunc) {

    clusterSize := len(servers)
    backends := make([]Backend, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := genBackend(servers[i], servers)
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
        assert.Nil(t, backend.Close())
    }
}

func TestMemoryBackendContruction(t *testing.T) {
    clusterSize := 3
    servers := ps.RandomMemoryServerAddrs(clusterSize)
    testBackendConstruction(t, servers, NewTestMemoryHSMBackend)
}

func TestSocketBackendConstruction(t *testing.T) {
    clusterSize := 3
    servers := ps.SetupSocketServerAddrs(clusterSize)
    testBackendConstruction(t, servers, NewTestSocketHSMBackend)
}

func TestRPCBackendContruction(t *testing.T) {
    clusterSize := 3
    servers := ps.SetupSocketServerAddrs(clusterSize)
    testBackendConstruction(t, servers, NewTestRPCHSMBackend)
}

func TestXXX(t *testing.T) {
    assert.Equal(t, hsm.EventType(100+4), ev.EventTerm)
    assert.Equal(t, hsm.EventType(1047), ev.EventClientUser)
    assert.Equal(t, hsm.EventType(1058), ev.EventNotifyPersistError)
}
