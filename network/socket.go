package network

import "net"
import "bufio"
import "github.com/ugorji/go/codec"
import hsm "github.com/hhkbp2/go-hsm"

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

func (self *SocketClient) RPC(requestType hsm.EventType, request interface{}, response interface{}) error {
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

// TODO add impl
type SocketServer struct {
    bindAddr net.Addr
    listener *net.TCPListener
    handler func(net.Conn)
}

func NewSocketServer(bindAddr net.Addr, handler func(net.Conn)) (*SocketServer, error) {
    listener, err := net.Listen(bindAddr.Network(), bindAddr.String())
    if err != nil {
        return nil, err
    }
    return &SocketServer{
        bindAddr: bindAddr,
        listener: listener,
        handler:handler,
    }
}

func (self *SocketServer) Server() {
    for {
        conn, err := self.
    }
}
