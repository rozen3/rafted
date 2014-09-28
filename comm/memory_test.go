package comm

import (
    "bufio"
    ev "github.com/zonas/rafted/event"
    logging "github.com/zonas/rafted/logging"
    ps "github.com/zonas/rafted/persist"
    "github.com/zonas/rafted/str"
    "github.com/hhkbp2/testify/assert"
    "github.com/ugorji/go/codec"
    "testing"
    "time"
)

var (
    testData     []byte = []byte(str.RandomString(100))
    testPoolSize        = 2
    testTimeout         = time.Millisecond * 50
)

func TestMemoryServerTransport(t *testing.T) {
    addr := ps.RandomMemoryServerAddr()
    register := NewMemoryTransportRegister()
    transport := NewMemoryServerTransport(&addr, testTimeout, register)
    transport.Open()
    sourceChan := make(chan []byte, 1)
    chunk := &TransportChunk{
        Data:     testData,
        SourceCh: sourceChan,
    }
    // test WriteChunk() and ReadChunk()
    err := transport.WriteChunk(chunk)
    assert.Nil(t, err)
    c, err := transport.ReadChunk()
    assert.Nil(t, err)
    assert.Equal(t, chunk, c)
    // test Read()
    err = transport.WriteChunk(chunk)
    assert.Nil(t, err)
    p := make([]byte, len(testData))
    n, err := transport.Read(p)
    assert.Nil(t, err)
    assert.Equal(t, n, len(testData))
    assert.Equal(t, testData, p)
    // test Write()
    n, err = transport.Write(testData)
    assert.Nil(t, err)
    assert.Equal(t, n, len(testData))
    select {
    case data := <-sourceChan:
        assert.Equal(t, testData, data)
    default:
        assert.True(t, false)
    }
    // test Close()
    err = transport.Close()
    assert.Nil(t, err)
}

func TestMemoryTransportRegister(t *testing.T) {
    var NilTransport *MemoryServerTransport
    testID := str.RandomString(10)
    register := NewMemoryTransportRegister()
    // test Register()
    transport, ok := register.Get(testID)
    assert.False(t, ok)
    register.Register(testID, NilTransport)
    transport, ok = register.Get(testID)
    assert.True(t, ok)
    assert.Equal(t, NilTransport, transport)
    // test Unregister()
    err := register.Unregister(testID)
    assert.Nil(t, err)
    transport, ok = register.Get(testID)
    assert.False(t, ok)
    // test Reset()
    register.Register(testID, NilTransport)
    id2 := str.RandomString(10)
    register.Register(id2, NilTransport)
    transport, ok = register.Get(testID)
    assert.True(t, ok)
    register.Reset()
    transport, ok = register.Get(testID)
    assert.False(t, ok)
    transport, ok = register.Get(id2)
    assert.False(t, ok)
}

func TestMemoryTransport(t *testing.T) {
    serverAddr := ps.RandomMemoryServerAddr()
    addr := &serverAddr
    register := NewMemoryTransportRegister()
    serverTran := NewMemoryServerTransport(addr, testTimeout, register)
    err := serverTran.Open()
    assert.Nil(t, err)
    clientTran := NewMemoryTransport(addr, testTimeout, register)
    // test PeerAddr()
    peerAddr := clientTran.PeerAddr()
    assert.Equal(t, addr, peerAddr)
    // test Open()
    err = clientTran.Open()
    assert.Nil(t, err)
    // test Write()
    n, err := clientTran.Write(testData)
    assert.Nil(t, err)
    assert.Equal(t, len(testData), n)
    p := make([]byte, len(testData))
    n, err = serverTran.Read(p)
    assert.Nil(t, err)
    assert.Equal(t, len(testData), n)
    assert.Equal(t, testData, p)
    n, err = serverTran.Write(p)
    assert.Nil(t, err)
    assert.Equal(t, len(p), n)
    // test Read()
    r := make([]byte, len(testData))
    n, err = clientTran.Read(r)
    assert.Nil(t, err)
    assert.Equal(t, len(p), n)
    // test Close()
    err = clientTran.Close()
    assert.Nil(t, err)
    serverTran.Close()
}

func getTestAppendEntriesEvents(
    addr ps.ServerAddr) (*ev.AppendEntriesRequestEvent, *ev.AppendEntriesResponseEvent) {
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  8,
            Index: 200,
            Type:  ps.LogNoop,
            Data:  testData,
            Conf:  nil,
        },
    }
    request := &ev.AppendEntriesRequest{
        Term:              100,
        Leader:            addr,
        PrevLogIndex:      199,
        PrevLogTerm:       8,
        Entries:           entries,
        LeaderCommitIndex: 173,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    response := &ev.AppendEntriesResponse{
        Term:         100,
        LastLogIndex: 165,
        Success:      true,
    }
    respEvent := ev.NewAppendEntriesResponseEvent(response)
    return reqEvent, respEvent
}

func startServerOnTran(
    t *testing.T,
    tran *MemoryServerTransport,
    reqEvent *ev.AppendEntriesRequestEvent,
    respEvent *ev.AppendEntriesResponseEvent) {

    go func() {
        reader := bufio.NewReader(tran)
        writer := bufio.NewWriter(tran)
        decoder := codec.NewDecoder(reader, &codec.MsgpackHandle{})
        encoder := codec.NewEncoder(writer, &codec.MsgpackHandle{})
        event, err := ReadRequest(reader, decoder)
        assert.Equal(t, event.Type(), ev.EventAppendEntriesRequest)
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        assert.True(t, ok)
        assert.Equal(t, reqEvent.Request, e.Request)
        err = WriteEvent(writer, encoder, respEvent)
        assert.Nil(t, err)
        tran.Close()
    }()
}

func TestMemoryConnection(t *testing.T) {
    serverAddr := ps.RandomMemoryServerAddr()
    addr := &serverAddr
    register := NewMemoryTransportRegister()
    serverTran := NewMemoryServerTransport(addr, testTimeout, register)
    err := serverTran.Open()
    assert.Nil(t, err)
    reqEvent, respEvent := getTestAppendEntriesEvents(serverAddr)
    startServerOnTran(t, serverTran, reqEvent, respEvent)
    conn := NewMemoryConnection(addr, testTimeout, register)
    // test Open()
    err = conn.Open()
    assert.Nil(t, err)
    // test CallRPC()
    event, err := conn.CallRPC(reqEvent)
    assert.Nil(t, err)
    assert.Equal(t, event.Type(), ev.EventAppendEntriesResponse)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    assert.True(t, ok)
    assert.Equal(t, respEvent.Response, e.Response)
    // test Close()
    err = conn.Close()
    assert.Nil(t, err)
}

func TestMemoryClient(t *testing.T) {
    serverAddr := ps.RandomMemoryServerAddr()
    register := NewMemoryTransportRegister()
    serverTran := NewMemoryServerTransport(&serverAddr, testTimeout, register)
    serverTran.Open()
    reqEvent, respEvent := getTestAppendEntriesEvents(serverAddr)
    startServerOnTran(t, serverTran, reqEvent, respEvent)
    client := NewMemoryClient(testPoolSize, testTimeout, register)
    // test CallRPCTo()
    event, err := client.CallRPCTo(&serverAddr, reqEvent)
    assert.Nil(t, err)
    assert.Equal(t, event.Type(), ev.EventAppendEntriesResponse)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    assert.True(t, ok)
    assert.Equal(t, respEvent.Response, e.Response)
    // test Close()
    err = client.Close()
    assert.Nil(t, err)
}

func TestMemoryServer(t *testing.T) {
    serverAddr := ps.RandomMemoryServerAddr()
    register := NewMemoryTransportRegister()
    logger := logging.GetLogger("test")
    reqEvent, respEvent := getTestAppendEntriesEvents(serverAddr)
    eventHandler := func(event ev.RequestEvent) {
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        assert.True(t, ok)
        assert.Equal(t, reqEvent.Request, e.Request)
        e.SendResponse(respEvent)
    }
    server := NewMemoryServer(&serverAddr, testTimeout, eventHandler, register, logger)
    server.Serve()
    client := NewMemoryClient(testPoolSize, testTimeout, register)
    event, err := client.CallRPCTo(&serverAddr, reqEvent)
    assert.Nil(t, err)
    assert.Equal(t, event.Type(), ev.EventAppendEntriesResponse)
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    assert.True(t, ok)
    assert.Equal(t, respEvent.Response, e.Response)
    // test Close()
    client.Close()
    err = server.Close()
    assert.Nil(t, err)
}
