package network

import "net"
import "bufio"
import "errors"
import "github.com/ugorji/go/codec"
import hsm "github.com/hhkbp2/go-hsm"
import rafted "github.com/hhkbp2/rafted"

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
    encoder *Decoder
    decoder *Encoder
}

func NewSocketConnection(addr net.Addr) *SocketConnection {
    return &SocketConnection{
        transport: NewSocketTransport(addr),
        reader:    bufio.NewReader(transport),
        writer:    bufio.NewWriter(transport),
        decoder:   codec.NewDecoder(reader, &codec.MsgpackHandle{}),
        encoder:   codec.NewEncoder(writer, &codec.MsgpackHandle{}),
    }
}

func (self *SocketConnection) CallRPC(request RaftEvent) (response RaftEvent, error) {
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
        connectionPool: make(map[string][]*SocketConnection),
        poolSize:       poolSize,
    }
}

func (self *SocketClient) CallRPC(target net.Addr, request, response RaftEvent) error {
    connection, err := self.getConnection(target)
    if err != nil {
        return err
    }

    err := connection.CallRPC(request, response)
    if err == nil {
        self.returnConnectionToPool(connection)
    }
    return err
}

func (self *SocketClient) getConnectionFromPool(target net.Addr) (*SocketConnection, error) {
    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    key := target.String()
    connections, ok := self.connectionPool[key]
    if !ok || len(conns) == 0 {
        return nil, errors.New("no connection for this target")
    }

    connection := connections[len(connections)-1]
    self.connectionPool[key] = connections[:len(connections)-1]
    return connection, nil
}

func (self *SocketClient) returnConnectionToPool(connection *SocketConnection) {
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

func (self *SocketClient) getConnection(target net.Addr) (*SocketConnection, error) {
    // check for pooled connection first
    if connection, err := self.getConnectionFromPool(target); connection != nil && err == nil {
        return connection
    }

    // if there is no pooled connection, create a new one
    connection := NewSocketConnection(target)
    if err := connection.Open(); err != nil {
        return nil, err
    }

    return connection
}

type SocketServer struct {
    bindAddr     net.Addr
    listener     *net.TCPListener
    eventHandler func(RequestEvent)
}

func NewSocketServer(bindAddr net.Addr, eventHandler func(RequestEvent)) (*SocketServer, error) {
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
        if err := self.handleCommand(reader, writer, decoder, encoder); err != nil {
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

func (self *SocketServer) handlCommand(reader *bufio.Reader, writer *bufio.Writer, decoder *Decoder, encoder *Encoder) error {
    // retrieve the request type as first byte
    requestType, err := reader.ReadByte()
    if err != nil {
        return err
    }

    // decode and handle the request
    resultChan := make(chan RaftEvent)
    switch requestType {
    case rafted.EventRequestVoteRequest:
        request := &msg.RequestVoteRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewRequestVoteRequestEvent(request, resultChan)
        self.eventHandler(event)
    case rafted.EventAppendEntriesRequest:
        request := &msg.AppendEntriesRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewAppendEntriesRequestEvent(request, resultChan)
        self.eventHandler(event)
    case rafted.EventPrepareInstallSnapshotRequest:
        request := &msg.PrepareInstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewPrepareInstallSnapshotRequestEvent(request, resultChan)
        self.eventHandler(event)
    case rafted.EventInstallSnapshotRequest:
        request := &msg.InstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewInstallSnapshotRequestEvent(request, resultChan)
        self.eventHandler(event)
    }

    // wait for response
    select {
    case response := <-resultChan:
        // write response type as one byte
        if err := writer.WriteByte(response.Type()); err != nil {
            return err
        }
        // write response content
        if err := encoder.Encode(response); err != nil {
            return err
        }
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

func WriteEvent(writer *bufio.Writer, encoder *Encoder, event RaftEvent) error {
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

func ReadRequest(reader *bufio.Reader, decoder *Decorder) (RequestEvent, error) {
    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch eventType {
    case rafted.EventAppendEntriesRequest:
        request := &rafted.AppendEntriesRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        event := rafted.NewAppendEntriesRequestEvent(request)
        return event, nil
    case rafted.EventRequestVoteRequest:
        request := &rafted.RequestVoteRequest{}
        if err := decoder.Decode(request); err != nil {
            return nil, err
        }
        return message, nil
        event := rafted.NewRequestVoteRequestEvent(request)
        return event, nil
    case rafted.EventPrepareInstallSnapshotRequest:
        request := &rafted.PrepareInstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewPrepareInstallSnapshotRequestEvent(request)
        return event, nil
    case rafted.EventInstallSnapshotRequest:
        request := &rafted.InstallSnapshotRequest{}
        if err := decoder.Decode(request); err != nil {
            return err
        }
        event := rafted.NewInstallSnapshotRequestEvent(request)
        return event, nil
    default:
        return nil, errors.New("not request event")
    }
}

func ReadResponse(reader *bufio.Reader, decoder *Decorder) (RaftEvent, error) {
    eventType, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch eventType {
    case rafted.EventAppendEntriesResponse:
        response := &rafted.AppendEntriesResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := rafted.NewAppendEntriesResponseEvent(response)
        return event, nil
    case rafted.EventRequestVoteResponse:
        response := &rafted.RequestVoteResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := rafted.NewRequestVoteResponseEvent(response)
        return event, nil
    case rafted.EventPrepareInstallSnapshotResponse:
        response := &rafted.PrepareInstallSnapshotResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
        event := rafted.NewPrepareInstallSnapshotResponseEvent(response)
        return event, nil
    case rafted.EventInstallSnapshotResponse:
        response := &rafted.InstallSnapshotResponse{}
        if err := decoder.Decode(response); err != nil {
            return nil, err
        }
    default:
        return nil, errors.New("not request event")
    }
}
