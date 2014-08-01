package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "sync"
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

type ClientEventListener struct {
    eventChan chan ev.ClientEvent
    stopChan  chan interface{}
    group     *sync.WaitGroup
}

func NewClientEventListener(ch chan ev.ClientEvent) *ClientEventListener {

    return &ClientEventListener{
        eventChan: ch,
        stopChan:  make(chan interface{}),
        group:     &sync.WaitGroup{},
    }
}

func (self *ClientEventListener) Start(fn func(ev.ClientEvent)) {
    self.group.Add(1)
    go self.start(fn)
}

func (self *ClientEventListener) start(fn func(ev.ClientEvent)) {
    defer self.group.Done()
    for {
        select {
        case <-self.stopChan:
            return
        case event := <-self.eventChan:
            fn(event)
        }
    }
}

func (self *ClientEventListener) Stop() {
    self.stopChan <- self
    self.group.Wait()
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
