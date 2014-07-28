package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    "net"
)

const (
    DefaultNotifyBufferSize = 100
)

type Notifier struct {
    notifyCh chan ev.NotifyEvent
}

func NewNotifier() *Notifier {
    return &Notifier{
        notifyCh: make(chan ev.NotifyEvent, DefaultNotifyBufferSize),
    }
}

func (self *Notifier) Notify(event ev.NotifyEvent) {
    select {
    case self.notifyCh <- event:
    default:
        // notifyCh not writable, ignore this event
        // TODO add log
    }
}

func (self *Notifier) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.notifyCh
}

func EncodeAddr(addr net.Addr) ([]byte, error) {
    if addr == nil {
        return nil, nil
    }
    return []byte(addr.String()), nil
}

func DecodeAddr(addr []byte) (net.Addr, error) {
    return net.ResolveTCPAddr("", string(addr))
}

// min returns the minimum.
func min(a, b uint64) uint64 {
    if a <= b {
        return a
    }
    return b
}

// max returns the maximum
func max(a, b uint64) uint64 {
    if a >= b {
        return a
    }
    return b
}
