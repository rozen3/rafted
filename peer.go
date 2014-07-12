package rafted

import "net"
import hsm "github.com/hhkbp2/go-hsm"
import state "github.com/hhkbp2/rafted/state"

const (
    HSMTypePeer = hsm.HSMTypeStd + 2 + iota
)

type PeerManager struct {
    addrs []net.Addr
    peers map[net.Addr]*Peer
}

func NewPeerManager(addrs []net.Addr, client network.Client, eventHandler func(RaftEvent)) *PeerManager {
    peers := make(map[net.Addr]*Peer)
    for _, addr := range addrs {
        peers[addr] = NewPeer(addr, client, eventHandler)
    }
    return &PeerManager{addrs, peers}
}

func (self *PeerManager) Broadcast(request RaftEvent) {
    for _, peer := range self.peers {
        peer.Send(request)
    }
}

func (self *PeerManager) PeerNumber() uint32 {
    return len(self.peers)
}

type Peer struct {
    hsm *PeerHSM
}

func NewPeer(addr net.Addr, client network.Client, eventHandler func(RaftEvent)) *Peer {
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, state.PeerStateID)
    state.NewPeerState(top)
    peerHSM := NewPeerHSM(top, initial, addr, client, eventHandler)
    return &Peer{peerHSM}
}

func (self *Peer) Start() {
    self.hsm.Init()
}

func (self *Peer) Send(request RaftEvent) {
    hsm.Dispatch(request)
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
    Client       network.Client
    EventHandler func(RaftEvent)
}

func NewPeerHSM(top, initial hsm.State, addr net.Addr, client network.Client, eventHandler func(RaftEvent)) *PeerHSM {
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
            if event.Type == EventTerm {
                return
            }
        case event := <-self.DispatchChan:
            self.StdHSM.Dispatch2(self, event)
        }
    }
}

func (self *PeerHSM) Dispatch(event hsm.Event) {
    self.Dispatch <- event
}

func (self *PeerHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTran2(self, target)
}

func (self *PeerHSM) SelfDispatchHSM(event hsm.Event) {
    self.SelfDispatchChan <- event
}

func (self *PeerHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(EventTerm))
    self.Group.Wait()
}
