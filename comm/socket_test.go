package comm

import (
    "bufio"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "github.com/ugorji/go/codec"
    "io"
    "net"
    "testing"
)

type MockSocketServer struct {
    host     string
    port     int
    listener net.Listener

    connHandler func(net.Conn)
}

func NewMockSocketServer(
    host string, port int, connHandler func(net.Conn)) (*MockSocketServer, error) {

    listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
    if err != nil {
        return nil, err
    }

    object := &MockSocketServer{
        host:        host,
        port:        port,
        listener:    listener,
        connHandler: connHandler,
    }
    return object, nil
}

func (self *MockSocketServer) Serve() {
    routine := func() {
        for {
            conn, err := self.listener.Accept()
            if err != nil {
                return
            }
            go self.connHandler(conn)
        }
    }
    go routine()
}

func (self *MockSocketServer) Close() error {
    return self.listener.Close()
}

const (
    TestSocketHost = "localhost"
    TestSocketPort = 33333
)

func TestSocketTransport(t *testing.T) {
    size := 100
    message := str.RandomString(uint32(size))
    handler := func(conn net.Conn) {
        defer conn.Close()
        buf := make([]byte, 1024)
        n, err := ReadN(conn, buf[:size])
        assert.True(t, (err == io.EOF) || (err == nil))
        assert.Equal(t, size, n)
        assert.Equal(t, message, string(buf[:n]))
        n, err = WriteN(conn, buf[:n])
        assert.Nil(t, err)
        assert.Equal(t, size, n)
    }
    server, err := NewMockSocketServer(TestSocketHost, TestSocketPort, handler)
    require.Nil(t, err)
    server.Serve()
    defer server.Close()
    addr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))

    transport := NewSocketTransport(addr, testTimeout)
    // test Open()
    err = transport.Open()
    require.Nil(t, err)
    // test Write()
    n, err := WriteN(transport, []byte(message))
    require.Nil(t, err)
    buf := make([]byte, size)
    // test Read()
    n, err = ReadN(transport, buf)
    require.Nil(t, err)
    require.Equal(t, size, n)
    require.Equal(t, message, string(buf))
    // test PeerAddr()
    peerAddr := transport.PeerAddr()
    require.Equal(t, addr.Network(), peerAddr.Network())
    require.Equal(t, addr.String(), peerAddr.String())
    // test Close()
    err = transport.Close()
    require.Nil(t, err)
}

func prepareRequestAndResponse() (*ev.AppendEntriesRequestEvent, *ev.AppendEntriesResponseEvent) {

    term := uint64(100)
    index := uint64(12004)
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  term,
            Index: index,
            Type:  ps.LogCommand,
            Data:  []byte(str.RandomString(50)),
            Conf: &ps.Config{
                Servers:    ps.RandomMemoryServerAddrs(5),
                NewServers: nil,
            },
        },
    }
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            ps.RandomMemoryServerAddr(),
        PrevLogIndex:      uint64(12003),
        PrevLogTerm:       uint64(99),
        Entries:           entries,
        LeaderCommitIndex: uint64(9998),
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    response := &ev.AppendEntriesResponse{
        Term:         term,
        LastLogIndex: uint64(12000),
        Success:      false,
    }
    respEvent := ev.NewAppendEntriesResponseEvent(response)
    return reqEvent, respEvent
}

func prepareConnHandler(
    t *testing.T,
    reqEvent *ev.AppendEntriesRequestEvent,
    respEvent *ev.AppendEntriesResponseEvent) func(conn net.Conn) {

    handler := func(conn net.Conn) {
        defer conn.Close()
        reader := bufio.NewReader(conn)
        writer := bufio.NewWriter(conn)
        decoder := codec.NewDecoder(reader, &codec.MsgpackHandle{})
        encoder := codec.NewEncoder(writer, &codec.MsgpackHandle{})
        event, err := ReadRequest(reader, decoder)
        require.Nil(t, err)
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        require.True(t, ok)
        require.Equal(t, reqEvent.Request, e.Request)
        err = WriteEvent(writer, encoder, respEvent)
        require.Nil(t, err)
    }
    return handler
}

func TestSocketConnection(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()
    handler := prepareConnHandler(t, reqEvent, respEvent)
    server, err := NewMockSocketServer(TestSocketHost, TestSocketPort, handler)
    require.Nil(t, err)
    server.Serve()
    defer server.Close()
    addr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))

    conn := NewSocketConnection(addr, testTimeout)
    // test Open()
    err = conn.Open()
    require.Nil(t, err)
    // test CallRPC()
    event, err := conn.CallRPC(reqEvent)
    require.True(t, (err == io.EOF) || (err == nil))
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)
    err = conn.Close()
    require.Nil(t, err)
}

func TestSocketClient(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()
    handler := prepareConnHandler(t, reqEvent, respEvent)
    server, err := NewMockSocketServer(TestSocketHost, TestSocketPort, handler)
    require.Nil(t, err)
    server.Serve()
    defer server.Close()
    addr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))

    poolSize := 10
    client := NewSocketClient(poolSize, testTimeout)
    event, err := client.CallRPCTo(addr, reqEvent)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)
    err = client.Close()
    require.Nil(t, err)
}

func TestSocketServer(t *testing.T) {
    reqEvent, respEvent := prepareRequestAndResponse()
    bindAddr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", "", TestSocketPort))
    require.Nil(t, err)
    serverAddr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))
    require.Nil(t, err)

    handler := func(event ev.RequestEvent) {
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        require.True(t, ok)
        require.Equal(t, reqEvent.Request, e.Request)
        e.SendResponse(respEvent)
    }
    logger := logging.GetLogger("test socket server")
    server, err := NewSocketServer(bindAddr, testTimeout, handler, logger)
    require.Nil(t, err)
    server.Serve()

    poolSize := 5
    client := NewSocketClient(poolSize, testTimeout)
    logger.Debug("here")
    event, err := client.CallRPCTo(serverAddr, reqEvent)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    require.True(t, ok)
    require.Equal(t, respEvent.Response, e.Response)
    err = client.Close()
    assert.Nil(t, err)
    err = server.Close()
    assert.Nil(t, err)
}
