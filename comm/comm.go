package comm

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "io"
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

    PeerAddr() ps.MultiAddr
    CallRPC(request ev.Event) (response ev.Event, err error)
}

type Client interface {
    CallRPCTo(
        target ps.MultiAddr, request ev.Event) (response ev.Event, err error)
    io.Closer
}

type Server interface {
    Serve()
    io.Closer
}

type RequestEventHandler func(ev.RequestEvent)
type EventHandler func(ev.Event)
