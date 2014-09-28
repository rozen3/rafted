package comm

import (
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "net"
    "testing"
)

func TestRPC(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()

    bindAddr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf(":%d", TestSocketPort))
    require.Nil(t, err)
    serverAddr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))
    require.Nil(t, err)

    handler := func(event ev.RequestEvent) {
        require.Equal(t, ev.EventAppendEntriesRequest, event.Type())
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        require.True(t, ok)
        assert.Equal(t, reqEvent.Request, e.Request)
        e.SendResponse(respEvent)
    }

    logger := logging.GetLogger("test rpc server")
    server, err := NewRPCServer(bindAddr, testTimeout, handler, logger)
    require.Nil(t, err)
    server.Serve()

    client := NewRPCClient(testTimeout)
    event, err := client.CallRPCTo(serverAddr, reqEvent)
    require.Nil(t, err)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)
    assert.Nil(t, client.Close())
    assert.Nil(t, server.Close())
}
