package rafted

import (
    //    cm "github.com/hhkbp2/rafted/comm"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/stretchr/testify/assert"
    "testing"
    "time"
)

// func SetupRedirectClient(
//     addr ps.ServerAddr,

// )

const (
    TestTimeout = time.Second * 5
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
    client := NewSimpleClient(backend, timeout)
    data := []byte("abcdefg")

    result, err := client.Append(data)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, data)

    result, err = client.ReadOnly(data)
    assert.Equal(t, err, nil)
    assert.Equal(t, result, data)

    oldServers := make([]ps.ServerAddr, 0)
    newServers := make([]ps.ServerAddr, 0)
    err = client.ChangeConfig(oldServers, newServers)
    assert.Equal(t, err, nil)
}

// func TestClient(t *testing.T) {
//     allAddrs := []ps.ServerAddr{
//         ps.ServerAddr{
//             Protocol: "memory",
//             IP:       "127.0.0.1",
//             Port:     6152,
//         },
//         ps.ServerAddr{
//             Protocol: "memory",
//             IP:       "127.0.0.1",
//             Port:     6153,
//         },
//     }

// }
