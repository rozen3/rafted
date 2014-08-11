package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "net"
    "sync"
    "time"
)

type PeerManager struct {
    peerAddrs []net.Addr
    peerMap   map[net.Addr]*Peer
    peerLock  sync.RWMutex
}

func NewPeerManager(
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    peerAddrs []net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local,
    getLoggerForPeer func(net.Addr) logging.Logger) *PeerManager {

    peerMap := make(map[net.Addr]*Peer)
    for _, addr := range peerAddrs {
        logger := getLoggerForPeer(addr)
        peerMap[addr] = NewPeer(
            heartbeatTimeout,
            maxAppendEntriesSize,
            maxSnapshotChunkSize,
            addr,
            client,
            eventHandler,
            local,
            logger)
    }
    object := &PeerManager{
        peerAddrs: peerAddrs,
        peerMap:   peerMap,
    }
    local.SetPeerManager(object)
    return object
}

func (self *PeerManager) Broadcast(event hsm.Event) {
    self.peerLock.RLock()
    defer self.peerLock.RUnlock()
    for _, peer := range self.peerMap {
        peer.Send(event)
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
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    addr net.Addr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local,
    logger logging.Logger) *Peer {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StatePeerID)
    peerState := NewPeerState(top, logger)
    NewDeactivatedPeerState(peerState, logger)
    activatedPeerState := NewActivatedPeerState(peerState, logger)
    NewCandidatePeerState(activatedPeerState, logger)
    leaderPeerState := NewLeaderPeerState(activatedPeerState, heartbeatTimeout, logger)
    NewStandardModePeerState(leaderPeerState, maxAppendEntriesSize, logger)
    NewSnapshotModePeerState(leaderPeerState, maxSnapshotChunkSize, logger)
    NewPipelineModePeerState(leaderPeerState, logger)
    peerHSM := NewPeerHSM(top, initial, addr, client, eventHandler, local)
    peerHSM.Init()
    return &Peer{peerHSM}
}

func (self *Peer) Start() {
    self.hsm.Init()
}

func (self *Peer) Send(event hsm.Event) {
    self.hsm.Dispatch(event)
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
        local:            local,
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

func (self *PeerHSM) Addr() net.Addr {
    return self.addr
}

func (self *PeerHSM) Client() comm.Client {
    return self.client
}

func (self *PeerHSM) EventHandler() func(ev.RaftEvent) {
    return self.eventHandler
}

func (self *PeerHSM) Local() *Local {
    return self.local
}
