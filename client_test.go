package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    rt "github.com/hhkbp2/rafted/retry"
    "github.com/stretchr/testify/assert"
    "testing"
    "time"
)

const (
    TestTimeout = time.Second * 5
    TestDelay   = time.Second * 2
)

var (
    TestData = []byte("abcdefg")
)

var (
    register = cm.NewMemoryTransportRegister()
)

type MockRaftBackend struct {
}

func NewMockRaftBackend() *MockRaftBackend {
    return &MockRaftBackend{}
}

func (self *MockRaftBackend) Send(event ev.RaftRequestEvent) {
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

func TestSimpleClient(t *testing.T) {
    backend := NewMockRaftBackend()
    timeout := TestTimeout
    retry := rt.NewOnceRetry(time.Sleep, time.Second*1)
    client := NewSimpleClient(backend, timeout, retry)

    result, err := client.Append(TestData)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, TestData)

    result, err = client.ReadOnly(TestData)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, TestData)

    oldServers := make([]ps.ServerAddr, 0)
    newServers := make([]ps.ServerAddr, 0)
    err = client.ChangeConfig(oldServers, newServers)
    assert.Equal(t, err, nil)
}

type MockRaftBackend2 struct {
    SendFunc func(event ev.RaftRequestEvent) ev.RaftEvent
}

func NewMockRaftBackend2(
    sendFunc func(event ev.RaftRequestEvent) ev.RaftEvent) *MockRaftBackend2 {

    return &MockRaftBackend2{
        SendFunc: sendFunc,
    }
}

func (self *MockRaftBackend2) Send(event ev.RaftRequestEvent) {
    respEvent := self.SendFunc(event)
    event.SendResponse(respEvent)
}

func setupTestRedirectClient(
    addr ps.ServerAddr,
    backend RaftBackend) *RedirectClient {

    eventHandler := func(event ev.RaftRequestEvent) {
        backend.Send(event)
    }
    logger := logging.GetLogger("Server" + "#" + addr.String())
    client := cm.NewMemoryClient(DefaultPoolSize, register)
    server := cm.NewMemoryServer(&addr, eventHandler, register, logger)
    logger2 := logging.GetLogger("RedirectClient" + "#" + addr.String())
    redirectRetry := rt.NewErrorRetry().
        MaxTries(3).
        Delay(time.Millisecond * 50)
    retry := redirectRetry.Copy().OnError(LeaderUnknown).OnError(LeaderUnsync)
    redirectClient := NewRedirectClient(
        TestTimeout, retry, redirectRetry, backend, client, server, logger2)
    redirectClient.Start()
    return redirectClient
}

func setupTestServerAddrs(number int) []ps.ServerAddr {
    addrs := make([]ps.ServerAddr, 0, number)
    for i := 0; i < number; i++ {
        addr := ps.ServerAddr{
            Protocol: "memory",
            IP:       "127.0.0.1",
            Port:     uint16(6152 + i),
        }
        addrs = append(addrs, addr)
    }
    return addrs
}

func TestRedirectClientContruction(t *testing.T) {
    register.Reset()
    addrs := setupTestServerAddrs(1)
    data := TestData
    invokedCount := 0
    backend := NewMockRaftBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount++
            response := &ev.ClientResponse{
                Success: true,
                Data:    data,
            }
            return ev.NewClientResponseEvent(response)
        })
    redirectClient := setupTestRedirectClient(addrs[0], backend)
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
    assert.Equal(t, result, nil)
    assert.Equal(t, invokedCount, 2)
}

func TestRedirectClientRedirection(t *testing.T) {
    register.Reset()
    addrs := setupTestServerAddrs(2)
    data := TestData
    invokedCount1 := 0
    backend1 := NewMockRaftBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount1++
            response := &ev.LeaderRedirectResponse{
                Leader: addrs[1],
            }
            return ev.NewLeaderRedirectResponseEvent(response)
        })
    redirectClient1 := setupTestRedirectClient(addrs[0], backend1)
    defer redirectClient1.Close()

    invokedCount2 := 0
    backend2 := NewMockRaftBackend2(
        func(event ev.RaftRequestEvent) ev.RaftEvent {
            invokedCount2++
            response := &ev.ClientResponse{
                Success: true,
                Data:    data,
            }
            return ev.NewClientResponseEvent(response)
        })
    redirectClient2 := setupTestRedirectClient(addrs[1], backend2)
    defer redirectClient2.Close()

    result, err := redirectClient1.Append(data)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, data)
    assert.Equal(t, invokedCount1, 1)
    assert.Equal(t, invokedCount2, 1)
}
