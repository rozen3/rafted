package rafted

import "net"
import hsm "github.com/hhkbp2/go-hsm"

const (
    HSMTypePeer = hsm.HSMTypeStd + 2 + iota
)

type Peer struct {
    hsm *PeerHSM
}

func NewPeer(addr net.Addr, client network.Client) *Peer {
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, PeerStateID)
    NewPeerState(top)
    peerHSM := NewPeerHSM(top, initial, client)
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
    Client network.Client
}

func NewPeerHSM(top, initial hsm.State, client network.Client) *PeerHSM {
    return &PeerHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypePeer, top, initial),
        DispatchChan:     make(chan hsm.Event, 1),
        SelfDispatchChan: make(chan hsm.Event, 1),
        client:           client,
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
