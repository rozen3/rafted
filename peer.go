package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "io"
    "strings"
    "sync"
    "time"
)

type Peers interface {
    Broadcast(event hsm.Event)
    AddPeers(peerAddrSlice *ps.ServerAddressSlice)
    RemovePeers(peerAddrSlice *ps.ServerAddressSlice)
    io.Closer
}

type PeerManager struct {
    peerMap  map[*ps.ServerAddress]Peer
    peerLock sync.RWMutex

    config           *Configuration
    client           cm.Client
    local            Local
    getLoggerForPeer func(ps.MultiAddr) logging.Logger
    logger           logging.Logger
}

func NewPeerManager(
    config *Configuration,
    client cm.Client,
    local Local,
    getLoggerForPeer func(ps.MultiAddr) logging.Logger,
    logger logging.Logger) *PeerManager {

    object := &PeerManager{
        peerMap:          make(map[*ps.ServerAddress]Peer),
        config:           config,
        client:           client,
        local:            local,
        getLoggerForPeer: getLoggerForPeer,
        logger:           logger,
    }
    local.SetPeers(object)
    return object
}

func (self *PeerManager) Broadcast(event hsm.Event) {
    self.peerLock.RLock()
    defer self.peerLock.RUnlock()
    self.logger.Debug("Broadcast(): %s", ev.EventString(event))
    for _, peer := range self.peerMap {
        peer.Send(event)
    }
}

func (self *PeerManager) AddPeers(peerAddrSlice *ps.ServerAddressSlice) {
    self.peerLock.Lock()
    defer self.peerLock.Unlock()
    self.logger.Debug("AddPeers(): %#v", peerAddrSlice)
    newPeerMap := AddrSliceToMap(peerAddrSlice)
    peersToAdd := MapSetMinus(newPeerMap, self.peerMap)
    self.logger.Debug(
        "peers to add: %#v", strings.Join(AddrsString(peersToAdd), " "))
    for _, addr := range peersToAdd {
        logger := self.getLoggerForPeer(addr)
        self.peerMap[addr] = NewPeerMan(
            self.config,
            addr,
            self.client,
            self.local,
            logger)
    }
}

func (self *PeerManager) RemovePeers(peerAddrSlice *ps.ServerAddressSlice) {
    self.peerLock.Lock()
    defer self.peerLock.Unlock()
    self.logger.Debug("RemovePeers(): %#v", peerAddrSlice)
    newPeerMap := AddrSliceToMap(peerAddrSlice)
    peersToRemove := MapSetMinus(self.peerMap, newPeerMap)
    self.logger.Debug(
        "peers to remove: %#v", strings.Join(AddrsString(peersToRemove), " "))
    for _, addr := range peersToRemove {
        peer, _ := self.peerMap[addr]
        peer.Close()
        delete(self.peerMap, addr)
    }
}

func (self *PeerManager) Close() error {
    self.peerLock.Lock()
    defer self.peerLock.Unlock()
    self.logger.Debug("PeerManager.Close()")
    for addr, peer := range self.peerMap {
        peer.Close()
        delete(self.peerMap, addr)
    }
    self.client.Close()
    return nil
}

type Peer interface {
    Send(Event hsm.Event)
    SendPrior(event hsm.Event)
    Close()

    QueryState() string
}

type PeerMan struct {
    peerHSM *PeerHSM
}

func NewPeerMan(
    config *Configuration,
    addr *ps.ServerAddress,
    client cm.Client,
    local Local,
    logger logging.Logger) Peer {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StatePeerID)
    peerState := NewPeerState(top, logger)
    NewDeactivatedPeerState(peerState, logger)
    activatedPeerState := NewActivatedPeerState(peerState, logger)
    NewCandidatePeerState(activatedPeerState, logger)
    leaderPeerState := NewLeaderPeerState(
        activatedPeerState,
        config.HeartbeatTimeout,
        config.MaxTimeoutJitter,
        logger)
    NewStandardModePeerState(leaderPeerState, config.MaxAppendEntriesSize, logger)
    NewSnapshotModePeerState(leaderPeerState, config.MaxSnapshotChunkSize, logger)
    NewPipelineModePeerState(leaderPeerState, logger)
    NewPersistErrorPeerState(peerState, logger)
    hsm.NewTerminal(top)
    peerHSM := NewPeerHSM(top, initial, addr, client, local)
    peerHSM.Init()
    return &PeerMan{peerHSM}
}

func (self *PeerMan) Send(event hsm.Event) {
    self.peerHSM.Dispatch(event)
}

func (self *PeerMan) SendPrior(event hsm.Event) {
    self.peerHSM.SelfDispatch(event)
}

func (self *PeerMan) Close() {
    self.peerHSM.Terminate()
}

func (self *PeerMan) QueryState() string {
    requestEvent := ev.NewQueryStateRequestEvent()
    self.peerHSM.Dispatch(requestEvent)
    responseEvent := requestEvent.RecvResponse()
    hsm.AssertEqual(responseEvent.Type(), ev.EventQueryStateResponse)
    event, ok := responseEvent.(*ev.QueryStateResponseEvent)
    hsm.AssertTrue(ok)
    return event.Response.StateID
}

type PeerHSM struct {
    *hsm.StdHSM
    dispatchChan     *ReliableEventChannel
    selfDispatchChan *ReliableEventChannel
    stopChan         chan interface{}
    group            sync.WaitGroup

    addr         *ps.ServerAddress
    client       cm.Client
    eventHandler cm.EventHandler
    local        Local
}

func NewPeerHSM(
    top hsm.State,
    initial hsm.State,
    addr *ps.ServerAddress,
    client cm.Client,
    local Local) *PeerHSM {

    handler := func(event ev.Event) {
        local.Send(event)
    }
    return &PeerHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypePeer, top, initial),
        dispatchChan:     NewReliableEventChannel(),
        selfDispatchChan: NewReliableEventChannel(),
        stopChan:         make(chan interface{}, 1),
        addr:             addr,
        client:           client,
        eventHandler:     handler,
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
        eventChan := self.dispatchChan.GetOutChan()
        for {
            select {
            case <-self.stopChan:
                return
            case event := <-priorityChan:
                self.StdHSM.Dispatch2(self, event)
                continue
            case <-time.After(0):
                // no event in priorityChan
            }
            select {
            case <-self.stopChan:
                return
            case event := <-priorityChan:
                self.StdHSM.Dispatch2(self, event)
            case event := <-eventChan:
                self.StdHSM.Dispatch2(self, event)
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *PeerHSM) Dispatch(event hsm.Event) {
    self.dispatchChan.Send(event)
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
    self.stopChan <- self
    self.group.Wait()
    self.dispatchChan.Close()
    self.selfDispatchChan.Close()
}

func (self *PeerHSM) Addr() *ps.ServerAddress {
    return self.addr
}

func (self *PeerHSM) Client() cm.Client {
    return self.client
}

func (self *PeerHSM) EventHandler() cm.EventHandler {
    return self.eventHandler
}

func (self *PeerHSM) Local() Local {
    return self.local
}
