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
    io.Closer

    io.Reader
    io.Writer
}

type Connection interface {
    Open() error
    io.Closer

    PeerAddr() net.Addr
    CallRPC(request event.RaftEvent) (response event.RaftEvent, err error)
}

type Client interface {
    CallRPCTo(
        target net.Addr,
        request event.RaftEvent) (response event.RaftEvent, err error)
    io.Closer
}

type Server interface {
    Serve()
    io.Closer
}

type RaftRequestEventHandler func(event.RaftRequestEvent)
type RaftEventHandler func(event.RaftEvent)
