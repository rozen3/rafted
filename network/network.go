package network

import "io"
import "net"
import hsm "github.com/hhkbp2/go-hsm"

type Encoder interface {
    Encode(e interface{}) error
}

type Decoder interface {
    Decode(e interface{}) error
}

type Transport interface {
    Open() error
    Close() error

    io.Reader
    io.Writer
}

// TODO to impl Transport
type BufferedTransport struct{}

// TODO to impl Transport
type MemoryTransport struct{}

// TODO to impl Transport
type FramedTransport struct{}

// TODO to impl Transport
type FileTransport struct{}

type Connection interface {
    Open() error
    Close() error

    PeerAddr() net.Addr
    SendRecv(requestType hsm.EventType, request interface{}, response interface{}) error
}

type Client interface {
    CallRPC(target net.Addr, requestType hsm.EventType, request interface{}, response interface{}) error
}

type Server interface {
    Serve()
}

type NetworkLayer interface {
    Client
    Server
}
