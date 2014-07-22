package comm

import "net"
import "bufio"
import "errors"
import "github.com/ugorji/go/codec"
import hsm "github.com/hhkbp2/go-hsm"
import event "github.com/hhkbp2/rafted/event"

type SocketTransport struct {
    addr net.Addr
    conn net.Conn
}

func NewSocketTransport(addr net.Addr) *SocketTransport {
    return &SocketTransport{addr}
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
    encoder *Encoder
    decoder *Decoder
}

func NewSocketConnection(addr net.Addr) *SocketConnection {
    return &SocketConnection{
        SocketTransport: NewSocketTransport(addr),
        reader:          bufio.NewReader(transport),
        writer:          bufio.NewWriter(transport),
        decoder:         codec.NewDecoder(reader, &codec.MsgpackHandle{}),
        encoder:         codec.NewEncoder(writer, &codec.MsgpackHandle{}),
    }
}

func (self *SocketConnection) CallRPC(
    request RaftEvent) (response RaftEvent, err error) {

    if err := WriteEvent(self.writer, self.encoder, request); err != nil {
        self.Close()
        return nil, err
    }

    if event, err := ReadReponse(self.reader, self.decoder); err != nil {
        self.Close()
        return nil, error
    }

    return event, nil
}

type SocketClient struct {
    connectionPool     map[net.Addr][]*SocketConnection
    connectionPoolLock sync.Mutex

    poolSize uint32
}

func NewSocketClient(poolSize uint32) *SocketClient {
    return &SocketClient{
        connectionPool: make(map[net.Addr][]*SocketConnection),
        poolSize:       poolSize,
    }
}

func (self *SocketClient) CallRPCTo(
    target net.Addr, request RaftEvent) (response RaftEvent, err error) {

    connection, err := self.getConnection(target)
    if err != nil {
        return nil, err
    }

    response, err := connection.CallRPC(request)
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

    key := target.String()
    connections, ok := self.connectionPool[key]

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
    connection := NewSocketConnection(target)
    if err := connection.Open(); err != nil {
        return nil, err
    }

    return connection, nil
}

type SocketServer struct {
    bindAddr     net.Addr
    listener     *net.TCPListener
    eventHandler func(RequestEvent)
}

func NewSocketServer(
    bindAddr net.Addr,
    eventHandler func(RequestEvent)) (*SocketServer, error) {

    listener, err := net.Listen(bindAddr.Network(), bindAddr.String())
    if err != nil {
        return nil, err
    }
    return &SocketServer{
        bindAddr:     bindAddr,
        listener:     listener,
        eventHandler: eventHandler,
    }
}

func (self *SocketServer) Serve() {
    for {
        conn, err := self.listener.Accept()
        if err != nil {
            // TODO add log
            continue
        }
        // TODO add log
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
                // TODO add log
            }
            return
        }
        if err := writer.Flush(); err != nil {
            // TODO add log
            return
        }
    }
}

func (self *SocketServer) handlCommand(
    reader *bufio.Reader,
    writer *bufio.Writer,
    decoder *Decoder,
    encoder *Encoder) error {

    // read request
    if event, err := ReadRequest(reader, decoder); err != nil {
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
    encoder *Encoder,
    event RaftEvent) error {
    // write event type as first byte
    if err := writer.WriteByte(event.Type()); err != nil {
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
    decoder *Decorder) (RequestEvent, error) {
    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch eventType {
    case event.EventAppendEntriesRequest:
        request := &event.AppendEntriesRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        event := event.NewAppendEntriesRequestEvent(request)
        return event, nil
    case event.EventRequestVoteRequest:
        request := &event.RequestVoteRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        return message, nil
        event := event.NewRequestVoteRequestEvent(request)
        return event, nil
    case event.EventInstallSnapshotRequest:
        request := &event.InstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := event.NewInstallSnapshotRequestEvent(request)
        return event, nil
    default:
        return nil, errors.New("not request event")
    }
}

func ReadResponse(
    reader *bufio.Reader,
    decoder *Decorder) (RaftEvent, error) {
    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch eventType {
    case event.EventAppendEntriesResponse:
        response := &event.AppendEntriesResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := event.NewAppendEntriesResponseEvent(response)
        return event, nil
    case event.EventRequestVoteResponse:
        response := &event.RequestVoteResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := event.NewRequestVoteResponseEvent(response)
        return event, nil
    case event.EventInstallSnapshotResponse:
        response := &event.InstallSnapshotResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
    default:
        return nil, errors.New("not request event")
    }
}
