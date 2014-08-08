package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    "github.com/hhkbp2/rafted/persist"
    "net"
    "sync"
    "sync/atomic"
)

const (
    HSMTypeRaft hsm.HSMType = hsm.HSMTypeStd + 1 + iota
    HSMTypePeer
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

    // configuration
    configManager persist.ConfigManager

    // state machine is exclusively used only in here
    stateMachine persist.StateMachine

    // log entries
    log persist.Log

    // snapshot
    snapshotManager persist.SnapshotManager

    // peers
    *PeerManager

    // notifier
    *Notifier
}

func NewLocalHSM(
    top, initial hsm.State,
    localAddr net.Addr,
    stateMachine persist.StateMachine,
    log persist.Log,
    snapshotManager persist.snapshotManager) *LocalHSM {

    return &LocalHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypeRaft, top, initial),
        dispatchChan:     make(chan hsm.Event, 1),
        selfDispatchChan: make(chan hsm.Event, 1),
        Group:            sync.WaitGroup{},
        localAddr:        localAddr,
        stateMachine:     stateMachine,
        log:              log,
        snapshotManager:  snapshotManager,
        Notifier:         NewNotifier(),
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
    return self.localAddr
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
    return self.ConfigManager
}

func (self *LocalHSM) Log() persist.Log {
    return self.log
}

func (self *LocalHSM) SnapshotManager() persist.SnapshotManager {
    return self.snapshotManager
}

func (self *LocalHSM) SetPeerManager(peerManager *PeerManager) {
    self.peerManager = peerManager
}

func (self *LocalHSM) QuorumSize() uint32 {
    return ((uint32(self.PeerManager.PeerNumber()) + 1) / 2) + 1
}

func (self *LocalHSM) ProcessLogsUpTo(index uint64) {
    // TODO add impl
    lastApplied := self.GetLastApplied()
    if index <= lastApplied {
        // TODO add log
        return
    }

    for i := lastApplied + 1; i <= index; i++ {
        if ret, err := self.ProcessLogAt(i); err != nil {
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
    self.log.StoreAppliedIndex(logEntry.Index)
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
    snapshotManager persist.snapshotManager) *Local {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateLocalID)
    localState := NewLocalState(top)
    followerState := NewFollowerState(localState, heartbeatTimeout)
    NewSnapshotRecoveryState(followerState)
    needPeersState := NewNeedPeersState(localState)
    NewCandidateState(needPeersState, electionTimeout)
    leaderState := NewLeaderState(needPeersState)
    NewUnsyncState(leaderState)
    NewSyncState(leaderState)
    localHSM := NewLocalHSM(
        top,
        initial,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager)
    localHSM.Init()
    return &Local{localHSM}
}
