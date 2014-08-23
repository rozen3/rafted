package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "sync"
    "time"
)

type PeerManager struct {
    peerMap  map[ps.ServerAddr]*Peer
    peerLock sync.RWMutex

    heartbeatTimeout     time.Duration
    maxAppendEntriesSize uint64
    maxSnapshotChunkSize uint64
    client               comm.Client
    eventHandler         func(ev.RaftEvent)
    local                *Local
    getLoggerForPeer     func(ps.ServerAddr) logging.Logger
}

func NewPeerManager(
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    peerAddrs []ps.ServerAddr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local,
    getLoggerForPeer func(ps.ServerAddr) logging.Logger) *PeerManager {

    peerMap := make(map[ps.ServerAddr]*Peer)
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
        peerMap:              peerMap,
        heartbeatTimeout:     heartbeatTimeout,
        maxAppendEntriesSize: maxAppendEntriesSize,
        maxSnapshotChunkSize: maxSnapshotChunkSize,
        client:               client,
        eventHandler:         eventHandler,
        local:                local,
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

func (self *PeerManager) AddPeers(localAddr ps.ServerAddr, conf *ps.Config) {
    self.peerLock.Lock()
    defer self.peerLock.Unlock()

    newPeerMap := self.genNewPeerMap(localAddr, conf)
    peersToAdd := MapSetMinus(newPeerMap, self.peerMap)
    for _, addr := range peersToAdd {
        logger := self.getLoggerForPeer(addr)
        self.peerMap[addr] = NewPeer(
            self.heartbeatTimeout,
            self.maxAppendEntriesSize,
            self.maxSnapshotChunkSize,
            addr,
            self.client,
            self.eventHandler,
            self.local,
            logger)
    }
}

func (self *PeerManager) RemovePeers(localAddr ps.ServerAddr, conf *ps.Config) {
    newPeerMap := self.genNewPeerMap(localAddr, conf)
    peersToRemove := MapSetMinus(self.peerMap, newPeerMap)
    for _, addr := range peersToRemove {
        peer, _ := self.peerMap[addr]
        peer.Close()
        delete(self.peerMap, addr)
    }

}

func (self *PeerManager) genNewPeerMap(
    localAddr ps.ServerAddr, conf *ps.Config) map[ps.ServerAddr]*Peer {

    newPeerMap := make(map[ps.ServerAddr]*Peer, 0)
    if conf.Servers != nil {
        for _, addr := range conf.Servers {
            if ps.AddrNotEqual(&addr, &localAddr) {
                newPeerMap[addr] = nil
            }
        }
    }
    if conf.NewServers != nil {
        for _, addr := range conf.NewServers {
            if ps.AddrNotEqual(&addr, &localAddr) {
                newPeerMap[addr] = nil
            }
        }
    }
    return newPeerMap
}

type Peer struct {
    hsm *PeerHSM
}

func NewPeer(
    heartbeatTimeout time.Duration,
    maxAppendEntriesSize uint64,
    maxSnapshotChunkSize uint64,
    addr ps.ServerAddr,
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
    hsm.NewTerminal(top)
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
    selfDispatchChan *ReliableEventChannel
    group            sync.WaitGroup

    addr         ps.ServerAddr
    client       comm.Client
    eventHandler func(ev.RaftEvent)
    local        *Local
}

func NewPeerHSM(
    top hsm.State,
    initial hsm.State,
    addr ps.ServerAddr,
    client comm.Client,
    eventHandler func(ev.RaftEvent),
    local *Local) *PeerHSM {

    return &PeerHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypePeer, top, initial),
        dispatchChan:     make(chan hsm.Event, 1),
        selfDispatchChan: NewReliableEventChannel(),
        addr:             addr,
        client:           client,
        eventHandler:     eventHandler,
        local:            local,
    }
}

func (self *PeerHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
    self.loop()
}

func (self *PeerHSM) loop() {
    routine := func() {
        defer self.group.Done()
        priorityChan := self.selfDispatchChan.GetOutChan()
        for {
            select {
            case event := <-priorityChan:
                self.StdHSM.Dispatch2(self, event)
            default:
                // no event in priorityChan
            }
            select {
            case event := <-self.dispatchChan:
                self.StdHSM.Dispatch2(self, event)
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *PeerHSM) Dispatch(event hsm.Event) {
    self.dispatchChan <- event
}

func (self *PeerHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSM(self, target)
}

func (self *PeerHSM) SelfDispatch(event hsm.Event) {
    self.selfDispatchChan.Send(event)
}

func (self *PeerHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.group.Wait()
    self.selfDispatchChan.Close()
}

func (self *PeerHSM) Addr() ps.ServerAddr {
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
