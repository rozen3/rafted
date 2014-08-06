package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    "net"
    "sync"
)

type PeerManager struct {
    peerAddrs []net.Addr
    peerMap   map[net.Addr]*Peer
    peerLock  sync.RWMutex
}

func NewPeerManager(
    peerAddrs []net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local) *PeerManager {

    peerMap := make(map[net.Addr]*Peer)
    for _, addr := range peerAddrs {
        peerMap[addr] = NewPeer(addr, client, eventHandler, local)
    }
    return &PeerManager{
        peerAddrs: peerAddrs,
        peerMap:   peerMap,
    }
}

func (self *PeerManager) Broadcast(request ev.RaftEvent) {
    self.peerLock.RLock()
    defer self.peerLock.RUnlock()
    for _, peer := range self.peerMap {
        peer.Send(request)
    }
}

func (self *PeerManager) PeerNumber() int {
    self.peerLock.RLock()
    defer self.peerLock.RUnlock()
    return len(self.peerAddrs)
}

type Peer struct {
    hsm *PeerHSM
}

func NewPeer(
    addr net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local) *Peer {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StatePeerID)
    peerState := NewPeerState(top)
    NewDeactivatedPeerState(peerState)
    activatedPeerState := NewActivatedPeerState(peerState)
    NewCandidatePeerState(activatedPeerState)
    leaderPeerState := NewLeaderPeerState(activatedPeerState)
    NewStandardModePeerState(leaderPeerState)
    NewSnapshotModePeerState(leaderPeerState)
    NewPipelineModePeerState(leaderPeerState)
    peerHSM := NewPeerHSM(top, initial, addr, client, eventHandler, local)
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
    dispatchChan     chan hsm.Event
    selfDispatchChan chan hsm.Event
    group            sync.WaitGroup

    addr         net.Addr
    client       comm.Client
    eventHandler func(ev.RaftEvent)
    local        *Local
}

func NewPeerHSM(
    top hsm.State,
    initial hsm.State,
    addr net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local) *PeerHSM {

    return &PeerHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypePeer, top, initial),
        dispatchChan:     make(chan hsm.Event, 1),
        selfDispatchChan: make(chan hsm.Event, 1),
        addr:             addr,
        client:           client,
        eventHandler:     eventHandler,
        local:            Local,
    }
}

func (self *PeerHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
    self.eventLoop()
}

func (self *PeerHSM) eventLoop() {
    self.group.Add(1)
    go self.loop()
}

func (self *PeerHSM) loop() {
    defer self.group.Done()
    for {
        select {
        case event := <-self.selfDispatchChan:
            self.StdHSM.Dispatch2(self, event)
            if event.Type() == ev.EventTerm {
                return
            }
        case event := <-self.dispatchChan:
            self.StdHSM.Dispatch2(self, event)
        }
    }
}

func (self *PeerHSM) Dispatch(event hsm.Event) {
    self.dispatchChan <- event
}

func (self *PeerHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSM(self, target)
}

func (self *PeerHSM) SelfDispatch(event hsm.Event) {
    self.selfDispatchChan <- event
}

func (self *PeerHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.group.Wait()
}

func (self *PeerHSM) GetLocal() *Local {
    return self.local
}
