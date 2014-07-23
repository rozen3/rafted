package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "sync"
)

const (
    HSMTypePeer = hsm.HSMTypeStd + 2 + iota
)

type PeerManager struct {
    addrs []net.Addr
    peers map[net.Addr]*Peer
}

func NewPeerManager(addrs []net.Addr, client comm.Client, eventHandler func(ev.RaftEvent)) *PeerManager {
    peers := make(map[net.Addr]*Peer)
    for _, addr := range addrs {
        peers[addr] = NewPeer(addr, client, eventHandler)
    }
    return &PeerManager{addrs, peers}
}

func (self *PeerManager) Broadcast(request ev.RaftEvent) {
    for _, peer := range self.peers {
        peer.Send(request)
    }
}

func (self *PeerManager) PeerNumber() int {
    return len(self.peers)
}

type Peer struct {
    hsm *PeerHSM
}

func NewPeer(
    addr net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent)) *Peer {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StatePeerID)
    NewPeerState(top)
    peerHSM := NewPeerHSM(top, initial, addr, client, eventHandler)
    return &Peer{peerHSM}
}

func (self *Peer) Start() {
    self.hsm.Init()
}

func (self *Peer) Send(request ev.RaftEvent) {
    self.hsm.Dispatch(request)
}

func (self *Peer) Close() {
    self.hsm.Terminate()
}

type PeerHSM struct {
    *hsm.StdHSM
    DispatchChan     chan hsm.Event
    SelfDispatchChan chan hsm.Event
    Group            sync.WaitGroup

    /* peer extanded fields */

    // network facility
    Addr         net.Addr
    Client       comm.Client
    EventHandler func(ev.RaftEvent)
}

func NewPeerHSM(
    top, initial hsm.State,
    addr net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent)) *PeerHSM {

    return &PeerHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypePeer, top, initial),
        DispatchChan:     make(chan hsm.Event, 1),
        SelfDispatchChan: make(chan hsm.Event, 1),
        Addr:             addr,
        Client:           client,
        EventHandler:     eventHandler,
    }
}

func (self *PeerHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
    self.eventLoop()
}

func (self *PeerHSM) eventLoop() {
    self.Group.Add(1)
    go self.loop()
}

func (self *PeerHSM) loop() {
    defer self.Group.Done()
    for {
        select {
        case event := <-self.SelfDispatchChan:
            self.StdHSM.Dispatch2(self, event)
            if event.Type() == ev.EventTerm {
                return
            }
        case event := <-self.DispatchChan:
            self.StdHSM.Dispatch2(self, event)
        }
    }
}

func (self *PeerHSM) Dispatch(event hsm.Event) {
    self.DispatchChan <- event
}

func (self *PeerHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTran2(self, target)
}

func (self *PeerHSM) SelfDispatch(event hsm.Event) {
    self.SelfDispatchChan <- event
}

func (self *PeerHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.Group.Wait()
}
