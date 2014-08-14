package rafted

import (
    "errors"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
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

// Min returns the minimum.
func Min(a, b uint64) uint64 {
    if a <= b {
        return a
    }
    return b
}

// Max returns the maximum
func Max(a, b uint64) uint64 {
    if a >= b {
        return a
    }
    return b
}

func MapSetMinus(
    s1 map[ps.ServerAddr]*Peer, s2 map[ps.ServerAddr]*Peer) []ps.ServerAddr {

    diff := make([]ps.ServerAddr, 0)
    for addr, _ := range s1 {
        if _, ok := s2[addr]; !ok {
            diff = append(diff, addr)
        }
    }
    return diff
}
