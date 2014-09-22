package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    rt "github.com/hhkbp2/rafted/retry"
    "github.com/hhkbp2/testify/assert"
    "testing"
    "time"
)

var (
    testRegister = cm.NewMemoryTransportRegister()
)

type MockBackend struct {
}

func NewMockBackend() *MockBackend {
    return &MockBackend{}
}

func (self *MockBackend) Send(event ev.RaftRequestEvent) {
    response := &ev.ClientResponse{
        Success: true,
    }
    switch event.Type() {
    case ev.EventClientAppendRequest:
        e, ok := event.(*ev.ClientAppendRequestEvent)
        hsm.AssertTrue(ok)
        response.Data = e.Request.Data
    case ev.EventClientReadOnlyRequest:
        e, ok := event.(*ev.ClientReadOnlyRequestEvent)
        hsm.AssertTrue(ok)
        response.Data = e.Request.Data
    default:
    }
    event.SendResponse(ev.NewClientResponseEvent(response))
}

func (self *MockBackend) Close() error {
    // empty body
    return nil
}

func (self *MockBackend) GetNotifyChan() <-chan ev.NotifyEvent {
    // dummy implementation
    return make(<-chan ev.NotifyEvent)
}

func TestSimpleClient(t *testing.T) {
    backend := NewMockBackend()
    timeout := testConfig.ClientTimeout
    retry := rt.NewOnceRetry(time.Sleep, time.Second*1)
    client := NewSimpleClient(backend, timeout, retry)

    result, err := client.Append(testData)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, testData)

    result, err = client.ReadOnly(testData)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, testData)

    oldServers := make([]ps.ServerAddr, 0)
    newServers := make([]ps.ServerAddr, 0)
    err = client.ChangeConfig(oldServers, newServers)
    assert.Equal(t, err, nil)
}

type MockBackend2 struct {
    SendFunc func(event ev.RaftRequestEvent) ev.RaftEvent
}

func NewMockBackend2(
    sendFunc func(event ev.RaftRequestEvent) ev.RaftEvent) *MockBackend2 {

    return &MockBackend2{
        SendFunc: sendFunc,
    }
}

func (self *MockBackend2) Send(event ev.RaftRequestEvent) {
    respEvent := self.SendFunc(event)
    event.SendResponse(respEvent)
}

func (self *MockBackend2) Close() error {
    // empty body
    return nil
}

func (self *MockBackend2) GetNotifyChan() <-chan ev.NotifyEvent {
    // dummy implementation
    return make(<-chan ev.NotifyEvent)
}

type GenRedirectClientFunc func(
    addr ps.ServerAddr, backend Backend) (*RedirectClient, error)

func setupTestMemoryRedirectClient(
    addr ps.ServerAddr, backend Backend) (*RedirectClient, error) {

    eventHandler := func(event ev.RaftRequestEvent) {
        backend.Send(event)
    }
    logger := logging.GetLogger("Server" + "#" + addr.String())
    client := cm.NewMemoryClient(
        testConfig.CommPoolSize, testConfig.CommTimeout, testRegister)
    server := cm.NewMemoryServer(
        &addr, testConfig.CommTimeout, eventHandler, testRegister, logger)
    logger2 := logging.GetLogger("RedirectClient" + "#" + addr.String())
    redirectRetry := rt.NewErrorRetry().
        MaxTries(3).
        Delay(testConfig.HeartbeatTimeout)
    retry := redirectRetry.Copy().OnError(LeaderUnknown).OnError(LeaderUnsync)
    redirectClient := NewRedirectClient(
        testConfig.ClientTimeout,
        retry,
        redirectRetry,
        backend,
        client,
        server,
        logger2)
    redirectClient.Start()
    return redirectClient, nil
}

func setupTestSocketRedirectClient(
    addr ps.ServerAddr, backend Backend) (*RedirectClient, error) {

    client := cm.NewSocketClient(
        testConfig.CommPoolSize, testConfig.CommTimeout)
    eventHandler := func(event ev.RaftRequestEvent) {
        backend.Send(event)
    }
    logger := logging.GetLogger("Server" + "#" + addr.String())
    server, err := cm.NewSocketServer(&addr, eventHandler, logger)
    if err != nil {
        return nil, err
    }
    logger2 := logging.GetLogger("RedirectClient" + "#" + addr.String())
    redirectRetry := rt.NewErrorRetry().
        MaxTries(3).
        Delay(testConfig.HeartbeatTimeout)
    retry := redirectRetry.Copy().OnError(LeaderUnknown).OnError(LeaderUnsync)
    redirectClient := NewRedirectClient(
        testConfig.ClientTimeout,
        retry,
        redirectRetry,
        backend,
        client,
        server,
        logger2)
    redirectClient.Start()
    return redirectClient, nil
}

func TestRedirectClientContruction(t *testing.T) {
    testRegister.Reset()
    addrs := ps.SetupMemoryServerAddrs(1)
    data := testData
    invokedCount := 0
    backend := NewMockBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount++
            response := &ev.ClientResponse{
                Success: true,
                Data:    data,
            }
            return ev.NewClientResponseEvent(response)
        })
    redirectClient, err := setupTestMemoryRedirectClient(addrs[0], backend)
    assert.Nil(t, err)
    defer redirectClient.Close()
    result, err := redirectClient.ReadOnly(data)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, data)
    assert.Equal(t, invokedCount, 1)
    backend.SendFunc = func(event ev.RaftRequestEvent) ev.RaftEvent {
        invokedCount++
        return ev.NewPersistErrorResponseEvent(errors.New("some error"))
    }
    result, err = redirectClient.ReadOnly(data)
    assert.Equal(t, err, PersistError)
    assert.Equal(t, result, []byte(nil))
    assert.Equal(t, invokedCount, 2)
}

func TestRedirectClientRedirection(t *testing.T) {
    testRegister.Reset()
    addrs := ps.SetupMemoryServerAddrs(2)
    data := testData
    invokedCount1 := 0
    backend1 := NewMockBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount1++
            response := &ev.LeaderRedirectResponse{
                Leader: addrs[1],
            }
            return ev.NewLeaderRedirectResponseEvent(response)
        })
    redirectClient1, err := setupTestMemoryRedirectClient(addrs[0], backend1)
    assert.Nil(t, err)
    defer redirectClient1.Close()

    invokedCount2 := 0
    backend2 := NewMockBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount2++
            response := &ev.ClientResponse{
                Success: true,
                Data:    data,
            }
            return ev.NewClientResponseEvent(response)
        })
    redirectClient2, err := setupTestMemoryRedirectClient(addrs[1], backend2)
    assert.Nil(t, err)
    defer redirectClient2.Close()

    result, err := redirectClient1.Append(data)
    assert.Equal(t, nil, err)
    assert.Equal(t, data, result)
    assert.Equal(t, 1, invokedCount1)
    assert.Equal(t, 1, invokedCount2)
}
