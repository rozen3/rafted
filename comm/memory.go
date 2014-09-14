package comm

import (
    "bufio"
    "bytes"
    "errors"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/ugorji/go/codec"
    "io"
    "net"
    "sync"
    "time"
)

const (
    DefaultTransportBufferSize = 16
)

var (
    MemoryTransportReadTimeout error = errors.New(
        "timeout on memory transport read")
    MemoryTransportWriteTimeout error = errors.New(
        "timeout on memory transport write")
)

type TransportChunk struct {
    Data     []byte
    SourceCh chan []byte
}

type MemoryServerTransport struct {
    addr       net.Addr
    timeout    time.Duration
    ConsumeCh  chan *TransportChunk
    ResponseCh chan []byte
    register   *MemoryTransportRegister
}

func NewMemoryServerTransport(
    addr net.Addr,
    timeout time.Duration,
    register *MemoryTransportRegister) *MemoryServerTransport {

    return &MemoryServerTransport{
        addr:      addr,
        ConsumeCh: make(chan *TransportChunk, DefaultTransportBufferSize),
        register:  register,
    }
}

func (self *MemoryServerTransport) Open() error {
    self.register.Register(self.addr.String(), self)
    return nil
}

func (self *MemoryServerTransport) ReadNextMessage() *TransportChunk {
    chunk := <-self.ConsumeCh
    return chunk
}

func (self *MemoryServerTransport) ReadChunk() (*TransportChunk, error) {
    chunk, ok := <-self.ConsumeCh
    if ok {
        return chunk, nil
    }
    return nil, io.EOF
}

func (self *MemoryServerTransport) WriteChunk(chunk *TransportChunk) error {
    select {
    case self.ConsumeCh <- chunk:
        return nil
    case <-time.After(self.timeout):
        return MemoryTransportWriteTimeout
    }
}

func BytesCopy(dest []byte, src []byte) int {
    i := 0
    for ; (i < len(dest)) && (i < len(src)); i++ {
        dest[i] = src[i]
    }
    return i
}

func (self *MemoryServerTransport) Read(p []byte) (int, error) {
    chunk, err := self.ReadChunk()
    if err != nil {
        return 0, err
    }
    n := BytesCopy(p, chunk.Data)
    self.ResponseCh = chunk.SourceCh
    return n, nil
}

func (self *MemoryServerTransport) Write(p []byte) (int, error) {
    if self.ResponseCh != nil {
        self.ResponseCh <- p
        return len(p), nil
    }
    return 0, errors.New("don't know where to response")
}

func (self *MemoryServerTransport) Close() error {
    close(self.ConsumeCh)
    if self.ResponseCh != nil {
        close(self.ResponseCh)
    }
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

func (self *MemoryTransportRegister) Reset() error {
    self.Lock()
    defer self.Unlock()
    self.transports = make(map[string]*MemoryServerTransport)
    return nil
}

func (self *MemoryTransportRegister) Get(
    id string) (transport *MemoryServerTransport, ok bool) {

    self.RLock()
    defer self.RUnlock()
    transport, ok = self.transports[id]
    return transport, ok
}

func (self *MemoryTransportRegister) String() string {
    self.RLock()
    defer self.RUnlock()
    var buf bytes.Buffer
    for k, _ := range self.transports {
        buf.WriteString(k + "\n")
    }
    return buf.String()
}

type MemoryTransport struct {
    addr      net.Addr
    timeout   time.Duration
    consumeCh chan []byte
    register  *MemoryTransportRegister
    peer      *MemoryServerTransport
}

func NewMemoryTransport(addr net.Addr, timeout time.Duration, register *MemoryTransportRegister) *MemoryTransport {
    return &MemoryTransport{
        addr:      addr,
        timeout:   timeout,
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
    select {
    case data := <-self.consumeCh:
        n := BytesCopy(b, data)
        return n, nil
    case <-time.After(self.timeout):
        return 0, MemoryTransportReadTimeout
    }
}

func (self *MemoryTransport) Write(b []byte) (int, error) {
    chunk := &TransportChunk{
        Data:     b,
        SourceCh: self.consumeCh,
    }
    if err := self.peer.WriteChunk(chunk); err != nil {
        return 0, err
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
    addr net.Addr,
    timeout time.Duration,
    register *MemoryTransportRegister) *MemoryConnection {

    conn := &MemoryConnection{
        MemoryTransport: NewMemoryTransport(addr, timeout, register),
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
    timeout  time.Duration
    register *MemoryTransportRegister
}

func NewMemoryClient(
    poolSize int,
    timeout time.Duration,
    register *MemoryTransportRegister) *MemoryClient {

    return &MemoryClient{
        connectionPool: make(map[string][]*MemoryConnection),
        poolSize:       poolSize,
        timeout:        timeout,
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

    connection = NewMemoryConnection(target, self.timeout, self.register)
    if err := connection.Open(); err != nil {
        return nil, err
    }

    return connection, nil
}

func (self *MemoryClient) Close() error {
    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()

    var err error
    for target, connections := range self.connectionPool {
        for _, connection := range connections {
            err = connection.Close()
        }
        delete(self.connectionPool, target)
    }
    return err
}

type MemoryServer struct {
    timeout             time.Duration
    register            *MemoryTransportRegister
    transport           *MemoryServerTransport
    acceptedConnections map[chan []byte]*MemoryServerTransport
    eventHandler        func(ev.RaftRequestEvent)
    logger              logging.Logger
}

func NewMemoryServer(
    bindAddr net.Addr,
    timeout time.Duration,
    eventHandler func(ev.RaftRequestEvent),
    register *MemoryTransportRegister,
    logger logging.Logger) *MemoryServer {

    transport := NewMemoryServerTransport(bindAddr, timeout, register)
    transport.Open()
    return &MemoryServer{
        timeout:             timeout,
        register:            register,
        transport:           transport,
        acceptedConnections: make(map[chan []byte]*MemoryServerTransport),
        eventHandler:        eventHandler,
        logger:              logger,
    }
}

func (self *MemoryServer) Serve() {
    for {
        chunk := self.transport.ReadNextMessage()
        if chunk == nil {
            self.logger.Debug("memory server read no more message")
            // server exit
            return
        }
        transport, ok := self.acceptedConnections[chunk.SourceCh]
        if ok {
            // connection already accepted
            transport.WriteChunk(chunk)
            continue
        } else {
            addr := ps.ServerAddr{
                Protocol: self.transport.Addr().Network(),
                IP: fmt.Sprintf(
                    "%s.%#v", self.transport.Addr().String(), chunk.SourceCh),
            }
            transport := NewMemoryServerTransport(&addr, self.timeout, self.register)
            transport.ResponseCh = chunk.SourceCh
            self.acceptedConnections[chunk.SourceCh] = transport
            transport.WriteChunk(chunk)
            go self.handleConn(transport)
        }
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
                self.logger.Error(
                    "memory server fails to handle command, error: %s", err)
            }
            return
        }
        if err := writer.Flush(); err != nil {
            self.logger.Error(
                "memory server fails to flush writer, error: %s", err)
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

func (self *MemoryServer) Close() error {
    return self.transport.Close()
}
