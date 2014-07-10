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

type SocketClient struct {
    *SocketTransport
    reader  *bufio.Reader
    writer  *bufio.Writer
    encoder *Decoder
    decoder *Encoder
}

func NewSocketClient(addr net.Addr) *SocketClient {
    return &SocketClient{
        transport: NewSocketTransport(addr),
        reader:    bufio.NewReader(transport),
        writer:    bufio.NewWriter(transport),
        decoder:   codec.NewDecoder(reader, &codec.MsgpackHandle{}),
        encoder:   codec.NewEncoder(writer, &codec.MsgpackHandle{}),
    }
}

func (self *SocketClient) CallRPC(requestType hsm.EventType, request interface{}, response interface{}) error {
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
    if err := self.decoder.Decode(response); err != nil {
        self.Close()
        return err
    }

    return nil
}

type SocketClientRPC struct {
    clientPool     map[net.Addr][]*SocketClient
    clientPoolLock sync.Mutex

    poolSize uint32
}

func NewSocketClientRPC(poolSize uint32) *SocketClientRPC {
    return &SocketClientRPC{
        clientPool: make(map[string][]*SocketClient),
        poolSize:   poolSize,
    }
}

func (self *SocketClientRPC) DoRPC(target net.Addr, requestType hsm.EventType, request interface{}, response interface{}) error {
    client, err := self.getClient(target)
    if err != nil {
        return err
    }

    err := client.CallRPC(requestType, request, response)
    if err == nil {
        self.returnClientToPool(client)
    }
    return err
}

func (self *SocketClientRPC) getClientFromPool(target net.Addr) (*SocketClient, error) {
    self.clientPoolLock.Lock()
    defer self.clientPoolLock.Unlock()

    key := target.String()
    clients, ok := self.clientPool[key]
    if !ok || len(conns) == 0 {
        return nil, errors.New("no client for this target")
    }

    client := clients[len(clients)-1]
    self.clientPool[key] = clients[:len(clients)-1]
    return client, nil
}

func (self *SocketClientRPC) returnClientToPool(client *SocketClient) {
    self.clientPoolLock.Lock()
    defer self.clientPoolLock.Unlock()

    key := target.String()
    clients, ok := self.clientPool[key]

    if len(clients) < self.poolSize {
        self.clientPool[key] = append(clients, client)
    } else {
        client.Close()
    }
}

func (self *SocketClientRPC) getClient(target net.Addr) (*SocketClient, error) {
    // check for pooled client first
    if client, err := self.getClientFromPool(target); client != nil && err == nil {
        return client
    }

    // if there is no pooled client, create a new one
    client := NewSocketClient(target)
    if err := client.Open(); err != nil {
        return nil, err
    }

    return client
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
        if err := self.handleCommand(reader, decoder, encoder); err != nil {
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

func (self *SocketServer) handlCommand(reader *bufio.Reader, decoder *Decoder, encoder *Encoder) error {
    // retrieve the request type as first byte
    requestType, err := reader.ReadByte()
    if err != nil {
        return err
    }

    // decode and handle the request
    resultChan := make(chan interface{})
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
        if err := encoder.Encode(response); err != nil {
            return err
        }
    }
    return nil
}
