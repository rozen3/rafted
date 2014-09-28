package comm

import (
    "io"
    "net"

    "github.com/zonas/rafted/event"
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
    CallRPC(request event.Event) (response event.Event, err error)
}

type Client interface {
    CallRPCTo(
        target net.Addr,
        request event.Event) (response event.Event, err error)
    io.Closer
}

type Server interface {
    Serve()
    io.Closer
}

type RequestEventHandler func(event.RequestEvent)
type EventHandler func(event.Event)
