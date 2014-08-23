package comm

import (
    "bufio"
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "github.com/ugorji/go/codec"
    "io"
    "net"
    "sync"
)

type SocketTransport struct {
    addr net.Addr
    conn net.Conn
}

func NewSocketTransport(addr net.Addr) *SocketTransport {
    return &SocketTransport{
        addr: addr,
    }
}

func (self *SocketTransport) Open() error {
    conn, err := net.Dial(self.addr.Network(), self.addr.String())
    if err != nil {
        return err
    }
    self.conn = conn
    return nil
}

func (self *SocketTransport) Close() error {
    return self.conn.Close()
}

func (self *SocketTransport) Read(b []byte) (int, error) {
    return self.conn.Read(b)
}

func (self *SocketTransport) Write(b []byte) (int, error) {
    return self.conn.Write(b)
}

func (self *SocketTransport) PeerAddr() net.Addr {
    return self.conn.RemoteAddr()
}

type SocketConnection struct {
    *SocketTransport
    reader  *bufio.Reader
    writer  *bufio.Writer
    encoder Encoder
    decoder Decoder
}

func NewSocketConnection(addr net.Addr) *SocketConnection {
    conn := &SocketConnection{
        SocketTransport: NewSocketTransport(addr),
    }
    conn.reader = bufio.NewReader(conn.SocketTransport)
    conn.writer = bufio.NewWriter(conn.SocketTransport)
    conn.decoder = codec.NewDecoder(conn.reader, &codec.MsgpackHandle{})
    conn.encoder = codec.NewEncoder(conn.writer, &codec.MsgpackHandle{})
    return conn
}

func (self *SocketConnection) CallRPC(
    request ev.RaftEvent) (response ev.RaftEvent, err error) {

    if err := WriteEvent(self.writer, self.encoder, request); err != nil {
        self.Close()
        return nil, err
    }

    event, err := ReadResponse(self.reader, self.decoder)
    if err != nil {
        self.Close()
        return nil, err
    }

    return event, nil
}

type SocketClient struct {
    connectionPool     map[string][]*SocketConnection
    connectionPoolLock sync.Mutex

    poolSize int
}

func NewSocketClient(poolSize int) *SocketClient {
    return &SocketClient{
        connectionPool: make(map[string][]*SocketConnection),
        poolSize:       poolSize,
    }
}

func (self *SocketClient) CallRPCTo(
    target net.Addr,
    request ev.RaftEvent) (response ev.RaftEvent, err error) {

    connection, err := self.getConnection(target)
    if err != nil {
        return nil, err
    }

    response, err = connection.CallRPC(request)
    if err == nil {
        self.returnConnectionToPool(connection)
    }
    return response, err
}

func (self *SocketClient) getConnectionFromPool(
    target net.Addr) (*SocketConnection, error) {

    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    key := target.String()
    connections, ok := self.connectionPool[key]
    if !ok || len(connections) == 0 {
        return nil, errors.New("no connection for this target")
    }

    connection := connections[len(connections)-1]
    self.connectionPool[key] = connections[:len(connections)-1]
    return connection, nil
}

func (self *SocketClient) returnConnectionToPool(
    connection *SocketConnection) {

    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    key := connection.PeerAddr().String()
    connections, ok := self.connectionPool[key]
    if !ok {
        connections = make([]*SocketConnection, 0)
        self.connectionPool[key] = connections
    }

    if len(connections) < self.poolSize {
        self.connectionPool[key] = append(connections, connection)
    } else {
        connection.Close()
    }
}

func (self *SocketClient) getConnection(
    target net.Addr) (*SocketConnection, error) {

    // check for pooled connection first
    connection, err := self.getConnectionFromPool(target)
    if connection != nil && err == nil {
        return connection, nil
    }

    // if there is no pooled connection, create a new one
    connection = NewSocketConnection(target)
    if err := connection.Open(); err != nil {
        return nil, err
    }

    return connection, nil
}

func (self *SocketClient) CloseAll(target net.Addr) error {
    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    key := target.String()
    connections, ok := self.connectionPool[key]
    if ok {
        var err error
        for _, connection := range connections {
            err = connection.Close()
        }
        delete(self.connectionPool, key)
        return err
    }
    return nil
}

type SocketServer struct {
    bindAddr     net.Addr
    listener     net.Listener
    eventHandler func(ev.RaftRequestEvent)
    logger       logging.Logger
}

func NewSocketServer(
    bindAddr net.Addr,
    eventHandler func(ev.RaftRequestEvent),
    logger logging.Logger) (*SocketServer, error) {

    listener, err := net.Listen(bindAddr.Network(), bindAddr.String())
    if err != nil {
        return nil, err
    }
    object := &SocketServer{
        bindAddr:     bindAddr,
        listener:     listener,
        eventHandler: eventHandler,
        logger:       logger,
    }
    return object, nil
}

func (self *SocketServer) Serve() {
    for {
        conn, err := self.listener.Accept()
        if err != nil {
            self.logger.Error("fail to accept connection")
            continue
        }
        go self.handleConn(conn)
    }
}

func (self *SocketServer) handleConn(conn net.Conn) {
    defer conn.Close()
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    decoder := codec.NewDecoder(reader, &codec.MsgpackHandle{})
    encoder := codec.NewEncoder(writer, &codec.MsgpackHandle{})

    for {
        if err := self.handleCommand(
            reader, writer, decoder, encoder); err != nil {

            if err != io.EOF {
                self.logger.Error(
                    "fail to handle command from connection: %s, error: %s",
                    conn.RemoteAddr().String(), err)
            }
            return
        }
        if err := writer.Flush(); err != nil {
            self.logger.Error("fail to write to connection: %s, error: %s",
                conn.RemoteAddr().String(), err)
            return
        }
    }
}

func (self *SocketServer) handleCommand(
    reader *bufio.Reader,
    writer *bufio.Writer,
    decoder Decoder,
    encoder Encoder) error {

    // read request
    event, err := ReadRequest(reader, decoder)
    if err != nil {
        return err
    }

    // dispatch event
    self.eventHandler(event)
    // wait for response
    response := event.RecvResponse()
    // send response
    if err := WriteEvent(writer, encoder, response); err != nil {
        return err
    }
    return nil
}

type SocketNetworkLayer struct {
    *SocketClient
    *SocketServer
}

func NewSocketNetworkLayer(
    client *SocketClient,
    server *SocketServer) *SocketNetworkLayer {
    return &SocketNetworkLayer{
        SocketClient: client,
        SocketServer: server,
    }
}

func WriteEvent(
    writer *bufio.Writer,
    encoder Encoder,
    event ev.RaftEvent) error {
    // write event type as first byte
    if err := writer.WriteByte(byte(event.Type())); err != nil {
        return err
    }
    // write the content of event
    if err := encoder.Encode(event.Message()); err != nil {
        return err
    }

    return writer.Flush()
}

func ReadRequest(
    reader *bufio.Reader,
    decoder Decoder) (ev.RaftRequestEvent, error) {

    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch hsm.EventType(eventType) {
    case ev.EventAppendEntriesRequest:
        request := &ev.AppendEntriesRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        event := ev.NewAppendEntriesRequestEvent(request)
        return event, nil
    case ev.EventRequestVoteRequest:
        request := &ev.RequestVoteRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        event := ev.NewRequestVoteRequestEvent(request)
        return event, nil
    case ev.EventInstallSnapshotRequest:
        request := &ev.InstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        event := ev.NewInstallSnapshotRequestEvent(request)
        return event, nil
    default:
        return nil, errors.New("not request event")
    }
}

func ReadResponse(
    reader *bufio.Reader,
    decoder Decoder) (ev.RaftEvent, error) {

    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch hsm.EventType(eventType) {
    case ev.EventAppendEntriesResponse:
        response := &ev.AppendEntriesResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := ev.NewAppendEntriesResponseEvent(response)
        return event, nil
    case ev.EventRequestVoteResponse:
        response := &ev.RequestVoteResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := ev.NewRequestVoteResponseEvent(response)
        return event, nil
    case ev.EventInstallSnapshotResponse:
        response := &ev.InstallSnapshotResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
    }
    return nil, errors.New("not request event")
}
