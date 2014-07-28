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
    // CandidateId that received vote in current term(or nil if none)
    votedFor     net.Addr
    votedForLock sync.RWMutex
    // log entries
    logLock sync.RWMutex
    log     persist.Log

    // the index of highest log entry known to be committed
    commitIndex uint64
    // the index of highest log entry applied to state machine
    lastApplied uint64

    // extanded fields
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
    self.StdHSM.QTran2(self, target)
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

func (self *RaftHSM) ApplyLogs() {
    // TODO add impl
}
