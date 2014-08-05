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
    HSMTypeRaft = hsm.HSMTypeStd + 1 + iota
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

type RaftHSM struct {
    *hsm.StdHSM
    DispatchChan     chan hsm.Event
    SelfDispatchChan chan hsm.Event
    Group            sync.WaitGroup

    /* raft extanded fields */
    // the current term
    currentTerm uint64

    // log entries
    log     persist.Log
    logLock sync.RWMutex

    // state machine
    stateMachine     persist.StateMachine
    stateMachineLock sync.RWMutex

    // the index of highest log entry known to be committed
    commitIndex uint64
    // the index of highest log entry applied to state machine
    lastApplied uint64

    // snapshot
    SnapshotManager persist.SnapshotManager
    // the latest snapshot term/index
    lastSnapshotTerm  uint64
    lastSnapshotIndex uint64

    // CandidateId that received vote in current term(or nil if none)
    votedFor     net.Addr
    votedForLock sync.RWMutex
    // leader infos
    leader     net.Addr
    leaderLock sync.RWMutex

    // local addr
    LocalAddr net.Addr
    // peers
    *PeerManager

    // notifier
    *Notifier
}

func NewRaftHSM(top, initial hsm.State, localAddr net.Addr) *RaftHSM {
    return &RaftHSM{
        StdHSM:           hsm.NewStdHSM(HSMTypeRaft, top, initial),
        DispatchChan:     make(chan hsm.Event, 1),
        SelfDispatchChan: make(chan hsm.Event, 1),
        Group:            sync.WaitGroup{},
        LocalAddr:        localAddr,
        Notifier:         NewNotifier(),
    }
}

func (self *RaftHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
    self.eventLoop()
}

func (self *RaftHSM) eventLoop() {
    self.Group.Add(1)
    go self.loop()
}

func (self *RaftHSM) loop() {
    defer self.Group.Done()
    // loop forever to process incoming event
    for {
        select {
        case event := <-self.SelfDispatchChan:
            // make `SelfDispatchChan' has higher priority
            // Event in this channel would be processed first
            self.StdHSM.Dispatch2(self, event)
            if event.Type() == ev.EventTerm {
                return
            }
        case event := <-self.DispatchChan:
            self.StdHSM.Dispatch2(self, event)
        }
    }
}

func (self *RaftHSM) Dispatch(event hsm.Event) {
    self.DispatchChan <- event
}

func (self *RaftHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSM(self, target)
}

func (self *RaftHSM) QTranOnEvent(targetStateID string, event hsm.Event) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSMOnEvent(self, target, event)
}

func (self *RaftHSM) SelfDispatch(event hsm.Event) {
    self.SelfDispatchChan <- event
}

func (self *RaftHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.Group.Wait()
}

func (self *RaftHSM) GetCurrentTerm() uint64 {
    return atomic.LoadUint64(&self.currentTerm)
}

func (self *RaftHSM) SetCurrentTerm(term uint64) {
    atomic.StoreUint64(&self.currentTerm, term)
}

func (self *RaftHSM) GetVotedFor() net.Addr {
    self.votedForLock.RLock()
    defer self.votedForLock.RUnlock()
    return self.votedFor
}

func (self *RaftHSM) SetVotedFor(votedFor net.Addr) {
    self.votedForLock.Lock()
    defer self.votedForLock.Unlock()
    self.votedFor = votedFor
}

func (self *RaftHSM) GetLog() persist.Log {
    return self.log
}

func (self *RaftHSM) GetLastTerm() uint64 {
    term, _ := self.log.LastTerm()
    return term
}

func (self *RaftHSM) GetLastIndex() uint64 {
    index, _ := self.log.LastIndex()
    return index
}

func (self *RaftHSM) GetLastLogInfo() (uint64, uint64) {
    return self.GetLastTerm(), self.GetLastIndex()
}

func (self *RaftHSM) GetCommitIndex() uint64 {
    return atomic.LoadUint64(&self.currentTerm)
}

func (self *RaftHSM) SetCommitIndex(index uint64) {
    atomic.StoreUint64(&self.commitIndex, index)
}

func (self *RaftHSM) GetLastApplied() uint64 {
    return atomic.LoadUint64(&self.currentTerm)
}

func (self *RaftHSM) SetLastApplied(index uint64) {
    atomic.StoreUint64(&self.lastApplied, index)
}

func (self *RaftHSM) GetSnapshotManager() persist.SnapshotManager {
    return self.SnapshotManager
}

func (self *RaftHSM) GetLastSnapshotTerm() uint64 {
    return atomic.LoadUint64(&self.lastSnapshotTerm)
}

func (self *RaftHSM) SetLastSnapshotTerm(term uint64) {
    atomic.StoreUint64(&self.lastSnapshotTerm, term)
}

func (self *RaftHSM) GetLastSnapshotIndex() uint64 {
    return atomic.LoadUint64(&self.lastSnapshotIndex)
}

func (self *RaftHSM) SetLastSnapshotIndex(index uint64) {
    atomic.StoreUint64(&self.lastSnapshotIndex, index)
}

func (self *RaftHSM) GetLeader() net.Addr {
    self.leaderLock.Lock()
    defer self.leaderLock.Unlock()
    return self.leader
}

func (self *RaftHSM) SetLeader(leader net.Addr) {
    self.leaderLock.Lock()
    defer self.leaderLock.Unlock()
    self.leader = leader
}

func (self *RaftHSM) SetPeerManager(peerManager *PeerManager) {
    self.PeerManager = peerManager
}

func (self *RaftHSM) QuorumSize() uint32 {
    return ((uint32(self.PeerManager.PeerNumber()) + 1) / 2) + 1
}

func (self *RaftHSM) ProcessLogsUpTo(index uint64) {
    // TODO add impl
    lastApplied := self.GetLastApplied()
    if index <= lastApplied {
        // TODO add log
        return
    }

    for i := lastApplied + 1; i <= index; i++ {
        self.ProcessLogAt(i)
    }
}

func (self *RaftHSM) ProcessLogAt(index uint64) []byte {
    logEntry, err := self.log.GetLog(index)
    if err != nil {
        // TODO add log
        // TODO change panic?
        panic(err)
    }
    result := self.processLog(logEntry)
    self.SetLastApplied(index)
    return result
}

func (self *RaftHSM) processLog(logEntry *persist.LogEntry) []byte {
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

func (self *RaftHSM) applyLog(logEntry *persist.LogEntry) []byte {
    return self.stateMachine.Apply(logEntry.Data)
}
