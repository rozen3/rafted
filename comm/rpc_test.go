package comm

import (
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "testing"
)

func TestRPC(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()
    bindAddr, serverAddr := prepareAddrs(t, TestSocketHost, TestSocketPort)

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

func TestRPCMultiServers(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()
    bindAddr1, serverAddr1 := prepareAddrs(t, TestSocketHost, TestSocketPort)
    bindAddr2, serverAddr2 := prepareAddrs(t, TestSocketHost, TestSocketPort+1)

    handler := func(event ev.RequestEvent) {
        require.Equal(t, ev.EventAppendEntriesRequest, event.Type())
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        require.True(t, ok)
        assert.Equal(t, reqEvent.Request, e.Request)
        e.SendResponse(respEvent)
    }

    logger1 := logging.GetLogger("test rpc server 1")
    logger2 := logging.GetLogger("test rpc server 2")
    server1, err := NewRPCServer(bindAddr1, testTimeout, handler, logger1)
    require.Nil(t, err)
    server1.Serve()
    server2, err := NewRPCServer(bindAddr2, testTimeout, handler, logger2)
    require.Nil(t, err)
    server2.Serve()

    client1 := NewRPCClient(testTimeout)
    event, err := client1.CallRPCTo(serverAddr1, reqEvent)
    require.Nil(t, err)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)

    client2 := NewRPCClient(testTimeout)
    event, err = client2.CallRPCTo(serverAddr2, reqEvent)
    require.Nil(t, err)
    e, ok = event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)

    assert.Nil(t, server1.Close())
    assert.Nil(t, server2.Close())
    assert.Nil(t, client1.Close())
    assert.Nil(t, client2.Close())
}
