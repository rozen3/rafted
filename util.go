package rafted

import (
    "errors"
    "fmt"
    ev "github.com/hhkbp2/rafted/event"
    "github.com/hhkbp2/rafted/persist"
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

func EncodeAddrs(addrs []net.Addr) ([]byte, error) {
    if addrs == nil {
        return nil, nil
    }
    var result bytes.Buffer
    for _, addr := range addrs {
        addrBin, err := EncodeAddr(addr)
        if err != nil {
            return nil, errors.New(
                fmt.Sprintf("fail to encode addr: %s", addr.String()))
        }
        if _, err := result.Write(addrBin); err != nil {
            return nil, err
        }
    }
    return result.Bytes(), nil
}

func EncodeConfig(conf *persist.Config) (*persist.Configuration, error) {
    if conf == nil {
        return nil, nil
    }
    ServersBin, err := EncodeAddrs(conf.Servers)
    if err != nil {
        return nil, err
    }
    NewServersBin, err := EncodeAddrs(conf.NewServers)
    if err != nil {
        return nil, err
    }
    conf := &persist.Configuration{
        Servers:    ServersBin,
        NewServers: NewServersBin,
    }
    return conf, nil
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

func IsNormalConfig(conf *persist.Config) bool {
    return (conf.Servers != nil) && (conf.NewServers == nil)
}

func IsOldNewConfig(conf *persist.Config) bool {
    return (conf.Servers != nil) && (conf.NewServers != nil)
}

func IsNewConfig(conf *persist.Config) bool {
    return (conf.Server == nil) && (conf.NewServers != nil)
}

func MapSetMinus(s1 map[net.Addr]*Peer, s2 map[net.Addr]*Peer) []net.Addr {
    diff := make([]net.Addr, 0)
    for addr, _ := range s1 {
        if _, ok := s2[addr]; !ok {
            diff = append(diff, addr)
        }
    }
    return diff
}
