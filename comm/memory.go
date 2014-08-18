package comm

import (
    "bufio"
    "errors"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/ugorji/go/codec"
    "io"
    "net"
    "sync"
)

const (
    DefaultTransportBufferSize = 16
)

type TransportChunk struct {
    Data     []byte
    SourceCh chan []byte
}

type MemoryServerTransport struct {
    addr       net.Addr
    ConsumeCh  chan *TransportChunk
    ResponseCh chan []byte
    register   *MemoryTransportRegister
}

func NewMemoryServerTransport(
    addr net.Addr, register *MemoryTransportRegister) *MemoryServerTransport {

    return &MemoryServerTransport{
        addr:       addr,
        ConsumeCh:  make(chan *TransportChunk, DefaultTransportBufferSize),
        ResponseCh: make(chan []byte),
        register:   register,
    }
}

func (self *MemoryServerTransport) Open() error {
    self.register.Register(self.addr.String(), self)
    return nil
}

func (self *MemoryServerTransport) ReadChunk() (*TransportChunk, error) {
    chunk := <-self.ConsumeCh
    return chunk, nil
}

func (self *MemoryServerTransport) WriteChunk(chunk *TransportChunk) error {
    self.ConsumeCh <- chunk
    return nil
}

func (self *MemoryServerTransport) Read(p []byte) (int, error) {
    chunk, _ := self.ReadChunk()
    p = chunk.Data
    return len(p), nil
}

func (self *MemoryServerTransport) Write(p []byte) (int, error) {
    self.ResponseCh <- p
    return len(p), nil
}

func (self *MemoryServerTransport) Close() error {
    return self.register.Unregister(self.addr.String())
}

func (self *MemoryServerTransport) Addr() net.Addr {
    return self.addr
}

type MemoryTransportRegister struct {
    sync.RWMutex
    transports map[string]*MemoryServerTransport
}

func NewMemoryTransportRegister() *MemoryTransportRegister {
    return &MemoryTransportRegister{
        transports: make(map[string]*MemoryServerTransport),
    }
}

func (self *MemoryTransportRegister) Register(
    id string, transport *MemoryServerTransport) {

    self.Lock()
    defer self.Unlock()
    self.transports[id] = transport
}

func (self *MemoryTransportRegister) Unregister(id string) error {
    self.Lock()
    defer self.Unlock()
    if _, ok := self.transports[id]; ok {
        delete(self.transports, id)
        return nil
    }
    return errors.New(fmt.Sprintf("no transport for id: %v", id))
}

func (self *MemoryTransportRegister) Get(
    id string) (transport *MemoryServerTransport, ok bool) {

    self.RLock()
    defer self.RUnlock()
    transport, ok = self.transports[id]
    return transport, ok
}

type MemoryTransport struct {
    addr      net.Addr
    consumeCh chan []byte
    register  *MemoryTransportRegister
    peer      *MemoryServerTransport
}

func NewMemoryTransport(addr net.Addr, register *MemoryTransportRegister) *MemoryTransport {
    return &MemoryTransport{
        addr:      addr,
        consumeCh: make(chan []byte, DefaultTransportBufferSize),
        register:  register,
    }
}

func (self *MemoryTransport) PeerAddr() net.Addr {
    return self.addr
}

func (self *MemoryTransport) Open() error {
    if transport, ok := self.register.Get(self.addr.String()); ok {
        self.peer = transport
        return nil
    }
    return errors.New(
        fmt.Sprintf("no server transport for id: %v", self.addr.String()))
}

func (self *MemoryTransport) Close() error {
    // empty body
    return nil
}

func (self *MemoryTransport) Read(b []byte) (int, error) {
    b = <-self.consumeCh
    return len(b), nil
}

func (self *MemoryTransport) Write(b []byte) (int, error) {
    chunk := &TransportChunk{b, self.consumeCh}
    if err := self.peer.WriteChunk(chunk); err != nil {
        return len(b), err
    }
    return len(b), nil
}

type MemoryConnection struct {
    *MemoryTransport
    reader  *bufio.Reader
    writer  *bufio.Writer
    encoder Encoder
    decoder Decoder
}

func NewMemoryConnection(
    addr net.Addr, register *MemoryTransportRegister) *MemoryConnection {

    conn := &MemoryConnection{
        MemoryTransport: NewMemoryTransport(addr, register),
    }
    conn.reader = bufio.NewReader(conn.MemoryTransport)
    conn.writer = bufio.NewWriter(conn.MemoryTransport)
    conn.decoder = codec.NewDecoder(conn.reader, &codec.MsgpackHandle{})
    conn.encoder = codec.NewEncoder(conn.writer, &codec.MsgpackHandle{})
    return conn
}

func (self *MemoryConnection) CallRPC(
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

type MemoryClient struct {
    connectionPool     map[string][]*MemoryConnection
    connectionPoolLock sync.Mutex

    poolSize int
    register *MemoryTransportRegister
}

func NewMemoryClient(
    poolSize int, register *MemoryTransportRegister) *MemoryClient {

    return &MemoryClient{
        connectionPool: make(map[string][]*MemoryConnection),
        poolSize:       poolSize,
        register:       register,
    }
}

func (self *MemoryClient) CallRPCTo(
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

func (self *MemoryClient) CloseAll(target net.Addr) error {

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

func (self *MemoryClient) getConnectionFromPool(
    target net.Addr) (*MemoryConnection, error) {

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

func (self *MemoryClient) returnConnectionToPool(
    connection *MemoryConnection) {

    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    key := connection.PeerAddr().String()
    connections, ok := self.connectionPool[key]
    if !ok {
        connections = make([]*MemoryConnection, 0)
        self.connectionPool[key] = connections
    }

    if len(connections) < self.poolSize {
        self.connectionPool[key] = append(connections, connection)
    } else {
        connection.Close()
    }
}

func (self *MemoryClient) getConnection(
    target net.Addr) (*MemoryConnection, error) {

    connection, err := self.getConnectionFromPool(target)
    if connection != nil && err == nil {
        return connection, nil
    }

    connection = NewMemoryConnection(target, self.register)
    if err := connection.Open(); err != nil {
        return nil, err
    }

    return connection, nil
}

type MemoryServer struct {
    transport           *MemoryServerTransport
    acceptedConnections map[chan []byte]*MemoryServerTransport

    eventHandler func(ev.RaftRequestEvent)
    register     *MemoryTransportRegister
    logger       logging.Logger
}

func NewMemoryServer(
    bindAddr net.Addr,
    eventHandler func(ev.RaftRequestEvent),
    register *MemoryTransportRegister,
    logger logging.Logger) *MemoryServer {

    transport := NewMemoryServerTransport(bindAddr, register)
    return &MemoryServer{
        transport:    transport,
        eventHandler: eventHandler,
        register:     register,
        logger:       logger,
    }
}

func (self *MemoryServer) Serve() {
    for {
        chunk, err := self.transport.ReadChunk()
        if err != nil {
            // TODO add log
            continue
        }
        transport, ok := self.acceptedConnections[chunk.SourceCh]
        if ok {
            // connection already accepted

            continue
        } else {
            addr := ps.ServerAddr{
                Protocol: self.transport.Addr().Network(),
                IP: fmt.Sprintf(
                    "%s.%#v", self.transport.Addr().String(), chunk.SourceCh),
            }
            transport := NewMemoryServerTransport(&addr, self.register)
            transport.ResponseCh = chunk.SourceCh
            self.acceptedConnections[chunk.SourceCh] = transport
            go self.handleConn(transport)
        }
        transport.WriteChunk(chunk)
    }
}

func (self *MemoryServer) handleConn(
    transport *MemoryServerTransport) {

    defer func() {
        transport.Close()
        delete(self.acceptedConnections, transport.ResponseCh)
    }()
    reader := bufio.NewReader(transport)
    writer := bufio.NewWriter(transport)
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

func (self *MemoryServer) handleCommand(
    reader *bufio.Reader,
    writer *bufio.Writer,
    decoder Decoder,
    encoder Encoder) error {

    event, err := ReadRequest(reader, decoder)
    if err != nil {
        return err
    }

    self.eventHandler(event)
    response := event.RecvResponse()
    if err := WriteEvent(writer, encoder, response); err != nil {
        return err
    }
    return nil
}
