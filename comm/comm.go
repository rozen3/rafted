package comm

import (
    "io"
    "net"

    "github.com/hhkbp2/rafted/event"
)

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

type Connection interface {
    Open() error
    Close() error

    PeerAddr() net.Addr
    CallRPC(request event.RaftEvent) (response event.RaftEvent, err error)
}

type Client interface {
    CallRPCTo(
        target net.Addr,
        request event.RaftEvent) (response event.RaftEvent, err error)
    CloseAll(target net.Addr) error
}

type Server interface {
    Serve()
}
