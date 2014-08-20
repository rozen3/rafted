package rafted

import (
    "container/list"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "sync"
)

// The general interface of event channel.
type EventChannel interface {
    Send(hsm.Event)
    Recv() hsm.Event
    Close()
}

// ReliableEventChannel is an unlimited size channel for
// non-blocking event sending/receiving.
type ReliableEventChannel struct {
    inChan    chan hsm.Event
    outChan   chan hsm.Event
    closeChan chan interface{}
    queue     *list.List
    group     *sync.WaitGroup
}

func NewReliableEventChannel() *ReliableEventChannel {
    object := &ReliableEventChannel{
        inChan:    make(chan hsm.Event, 1),
        outChan:   make(chan hsm.Event, 1),
        closeChan: make(chan interface{}, 1),
        queue:     list.New(),
        group:     &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *ReliableEventChannel) Start() {
    routine := func() {
        defer self.group.Done()
        for {
            if self.queue.Len() > 0 {
                e := self.queue.Front()
                outEvent, _ := e.Value.(hsm.Event)
                select {
                case <-self.closeChan:
                    return
                case inEvent := <-self.inChan:
                    self.queue.PushBack(inEvent)
                case self.outChan <- outEvent:
                    self.queue.Remove(e)
                }
            } else {
                select {
                case <-self.closeChan:
                    return
                case event := <-self.inChan:
                    self.queue.PushBack(event)
                }
            }
        }
    }
    self.startRoutine(routine)
}

func (self *ReliableEventChannel) startRoutine(fn func()) {
    self.group.Add(1)
    go fn()
}

func (self *ReliableEventChannel) Send(event hsm.Event) {
    self.inChan <- event
}

func (self *ReliableEventChannel) Recv() hsm.Event {
    event := <-self.outChan
    return event
}

func (self *ReliableEventChannel) GetInChan() chan<- hsm.Event {
    return self.inChan
}

func (self *ReliableEventChannel) GetOutChan() <-chan hsm.Event {
    return self.outChan
}

func (self *ReliableEventChannel) Close() {
    self.closeChan <- self
    self.group.Wait()
}

// Notifier is use to signal notify to the outside of this module.
type Notifier struct {
    inChan    *ReliableEventChannel
    outChan   chan ev.NotifyEvent
    closeChan chan interface{}
    group     *sync.WaitGroup
}

func NewNotifier() *Notifier {
    object := &Notifier{
        inChan:  NewReliableEventChannel(),
        outChan: make(chan ev.NotifyEvent, 1),
        group:   &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *Notifier) Start() {
    self.group.Add(1)
    go func() {
        defer self.group.Done()
        inChan := self.inChan.GetOutChan()
        for {
            select {
            case <-self.closeChan:
                return
            case event := <-inChan:
                ne, _ := event.(ev.NotifyEvent)
                self.outChan <- ne
            }
        }
    }()
}

func (self *Notifier) Notify(event ev.NotifyEvent) {
    self.inChan.Send(event)
}

func (self *Notifier) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.outChan
}

func (self *Notifier) Close() {
    self.closeChan <- self
    self.group.Wait()
    self.inChan.Close()
}

// ClientEventListener is a a helper class for listening client response
// in independent go routine.
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

type Applier struct {
    log          ps.Log
    stateMachine ps.StateMachine
}

func NewApplier(log ps.Log, stateMachine ps.StateMachine) *Applier {
    return &Applier{
        log:          log,
        stateMachine: stateMachine,
    }
}

func (self *Applier) FollowerCommit(index uint64) {
    // TODO add impl
}

// Min returns the minimum.
func Min(a, b uint64) uint64 {
    if a <= b {
        return a
    }
    return b
}

// Max returns the maximum.
func Max(a, b uint64) uint64 {
    if a >= b {
        return a
    }
    return b
}

// MapSetMinus calculates the difference of two map, and returns
// the result of s1 - s2.
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
