package network

import "net"
import "bufio"
import "github.com/ugorji/go/codec"
import hsm "github.com/hhkbp2/go-hsm"
import rafted "github.com/hhkbp2/rafted"
import msg "github.com/hhkbp2/rafted/message"

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

func (self *SocketConnection) SendRecv(requestType hsm.EventType, request interface{}, response interface{}) error {
    // write request type as one byte
    if err := self.writer.WriteByte(requestType); err != nil {
        self.Close()
        return err
    }
    // write the content of request
    if err := conn.encoder.Encode(request); err != nil {
        self.Close()
        return err
    }
    // flush the request into network
    if err := self.writer.Flush(); err != nil {
        self.Close()
        return err
    }

    // read the response
    // read the response type as first byte
    if responseType, err := self.reader.ReadByte(); err != nil {
        self.Close()
        return err
    }
    // read the response content
    if err := self.decoder.Decode(response); err != nil {
        self.Close()
        return err
    }

    return nil
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

func (self *SocketClient) CallRPC(target net.Addr, requestType hsm.EventType, request interface{}, response interface{}) error {
    connection, err := self.getConnection(target)
    if err != nil {
        return err
    }

    err := connection.CallRPC(requestType, request, response)
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
    eventHandler func(*rafted.RequestEvent)
}

func NewSocketServer(bindAddr net.Addr, eventHandler func(*rafted.RequestEvent)) (*SocketServer, error) {
    listener, err := net.Listen(bindAddr.Network(), bindAddr.String())
    if err != nil {
        return nil, err
    }
    return &SocketServer{
        bindAddr: bindAddr,
        listener: listener,
        handler:  handler,
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
    resultChan := make(chan hsm.Event)
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

type SocketRaftNode struct {
    *SocketClient
    *SocketServer
}

func NewSocketRaftNode(
    client *SocketClient,
    server *SocketServer) *SocketRaftNode {
    return &SocketRaftNode{
        SocketClient: client,
        SocketServer: server,
    }
}
