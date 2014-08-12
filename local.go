package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "github.com/hhkbp2/rafted/persist"
    "net"
    "sync"
    "sync/atomic"
    "time"
)

const (
    HSMTypeRaft hsm.HSMType = hsm.HSMTypeStd + 1 + iota
    HSMTypePeer
)

type MemberChangeStatusType uint8

const (
    MemberChangeStatusNotSet MemberChangeStateType = iota
    NotInMemeberChange
    OldNewConfigSeen
    OldNewConfigCommitted
    NewConfigSeen
    NewConfigCommitted
)

type SelfDispatchHSM interface {
    hsm.HSM
    SelfDispatch(event hsm.Event)
}

type TerminableHSM interface {
    hsm.HSM
    Terminate()
}

type NotifiableHSM interface {
    hsm.HSM
    GetNotifyChan() <-chan ev.NotifyEvent
}

type LocalHSM struct {
    *hsm.StdHSM
    dispatchChan     chan hsm.Event
    selfDispatchChan chan hsm.Event
    group            sync.WaitGroup

    // the current term
    currentTerm uint64

    // the local addr
    localAddr     net.Addr
    localAddrLock sync.RWMutex

    // CandidateId that received vote in current term(or nil if none)
    votedFor     net.Addr
    votedForLock sync.RWMutex
    // leader infos
    leader     net.Addr
    leaderLock sync.RWMutex

    // member change infos
    memberChangeStatus MemberChangeStatusType

    // configuration
    configManager persist.ConfigManager

    // state machine is exclusively used only in here
    stateMachine persist.StateMachine

    // log entries
    log persist.Log

    // snapshot
    snapshotManager persist.SnapshotManager

    // peers
    peerManager *PeerManager

    // notifier
    *Notifier

    logging.Logger
}

func NewLocalHSM(
    top hsm.State,
    initial hsm.State,
    localAddr net.Addr,
    configManager persist.ConfigManager,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.SnapshotManager,
    logger logging.Logger) (*LocalHSM, error) {

    if conf, err := configManager.LastConfig(); err != nil {
        return nil, err
    }
    if !persist.IsInMemeberChange(conf) {

    }
    // TODO check the integrety between log and configManager

    // memberChangeStatus

    return &LocalHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypeRaft, top, initial),
        dispatchChan:     make(chan hsm.Event, 1),
        selfDispatchChan: make(chan hsm.Event, 1),
        group:            sync.WaitGroup{},
        localAddr:        localAddr,
        configManager:    configManager,
        stateMachine:     stateMachine,
        log:              log,
        snapshotManager:  snapshotManager,
        Notifier:         NewNotifier(),
        Logger:           logger,
        //    memberChangeStatus:
    }
}

func (self *LocalHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
    self.eventLoop()
}

func (self *LocalHSM) eventLoop() {
    self.group.Add(1)
    go self.loop()
}

func (self *LocalHSM) loop() {
    defer self.group.Done()
    // loop forever to process incoming event
    for {
        select {
        case event := <-self.selfDispatchChan:
            // make selfDispatchChan has higher priority
            // Event in this channel would be processed first
            self.StdHSM.Dispatch2(self, event)
            if event.Type() == ev.EventTerm {
                return
            }
        case event := <-self.dispatchChan:
            self.StdHSM.Dispatch2(self, event)
        }
    }
}

func (self *LocalHSM) Dispatch(event hsm.Event) {
    self.dispatchChan <- event
}

func (self *LocalHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSM(self, target)
}

func (self *LocalHSM) QTranOnEvent(targetStateID string, event hsm.Event) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSMOnEvent(self, target, event)
}

func (self *LocalHSM) SelfDispatch(event hsm.Event) {
    self.selfDispatchChan <- event
}

func (self *LocalHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.group.Wait()
}

func (self *LocalHSM) GetCurrentTerm() uint64 {
    return atomic.LoadUint64(&self.currentTerm)
}

func (self *LocalHSM) SetCurrentTerm(term uint64) {
    atomic.StoreUint64(&self.currentTerm, term)
}

func (self *LocalHSM) GetLocalAddr() net.Addr {
    self.localAddrLock.RLock()
    defer self.localAddrLock.RUnlock()
    return self.localAddr
}

func (self *LocalHSM) SetLocalAddr(addr net.Addr) {
    self.localAddrLock.Lock()
    defer self.localAddrLock.Unlock()
    self.localAddr = addr
}

func (self *LocalHSM) GetVotedFor() net.Addr {
    self.votedForLock.RLock()
    defer self.votedForLock.RUnlock()
    return self.votedFor
}

func (self *LocalHSM) SetVotedFor(votedFor net.Addr) {
    self.votedForLock.Lock()
    defer self.votedForLock.Unlock()
    self.votedFor = votedFor
}

func (self *LocalHSM) GetLeader() net.Addr {
    self.leaderLock.RLock()
    defer self.leaderLock.RUnlock()
    return self.leader
}

func (self *LocalHSM) SetLeader(leader net.Addr) {
    self.leaderLock.Lock()
    defer self.leaderLock.Unlock()
    self.leader = leader
}

func (self *LocalHSM) ConfigManager() persist.ConfigManager {
    return self.configManager
}

func (self *LocalHSM) Log() persist.Log {
    return self.log
}

func (self *LocalHSM) SnapshotManager() persist.SnapshotManager {
    return self.snapshotManager
}

func (self *LocalHSM) PeerManager() *PeerManager {
    return self.peerManager
}

func (self *LocalHSM) SetPeerManager(peerManager *PeerManager) {
    self.peerManager = peerManager
}

func (self *LocalHSM) QuorumSize() uint32 {
    return ((uint32(self.peerManager.PeerNumber()) + 1) / 2) + 1
}

func (self *LocalHSM) ProcessLogsUpTo(index uint64) {
    // TODO add impl
    lastAppliedIndex, err := self.log.LastAppliedIndex()
    if err != nil {
        // TODO error handing
    }
    if index <= lastAppliedIndex {
        // TODO add log
        return
    }

    for i := lastAppliedIndex + 1; i <= index; i++ {
        if _, err := self.ProcessLogAt(i); err != nil {
            // TODO error handling
        }
    }
}

func (self *LocalHSM) ProcessLogAt(index uint64) ([]byte, error) {
    logEntry, err := self.log.GetLog(index)
    if err != nil {
        // TODO add log
        // TODO change panic?
        return nil, err
    }
    return self.processLog(logEntry), nil
}

func (self *LocalHSM) processLog(logEntry *persist.LogEntry) []byte {
    switch logEntry.Type {
    case persist.LogCommand:
        // TODO add impl
        return self.applyLog(logEntry)
    case persist.LogNoop:
        // just ignore

        /* TODO add other types */

    default:
        // unknown log entry type
        // TODO add log
    }
    return make([]byte, 0)
}

func (self *LocalHSM) applyLog(logEntry *persist.LogEntry) []byte {
    result := self.stateMachine.Apply(logEntry.Data)
    self.log.StoreLastAppliedIndex(logEntry.Index)
    return result
}

type Local struct {
    *LocalHSM
}

func NewLocal(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr,
    configManager persist.ConfigManager,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.SnapshotManager,
    logger logging.Logger) *Local {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateLocalID)
    localState := NewLocalState(top, logger)
    followerState := NewFollowerState(localState, heartbeatTimeout, logger)
    NewSnapshotRecoveryState(followerState, logger)
    needPeersState := NewNeedPeersState(localState, logger)
    NewCandidateState(needPeersState, electionTimeout, logger)
    leaderState := NewLeaderState(needPeersState, logger)
    NewUnsyncState(leaderState, logger)
    NewSyncState(leaderState, logger)
    localHSM := NewLocalHSM(
        top,
        initial,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        logger)
    localHSM.Init()
    return &Local{localHSM}
}
