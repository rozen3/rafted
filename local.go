package rafted

import (
    "errors"
    "fmt"
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
    memberChangeStatus     MemberChangeStatusType
    memberChangeStatusLock sync.RWMutex

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

    // TODO check the integrety between log and configManager
    // try to fix the inconsistant base on log status.
    // Return error if fail to do that

    memberChangeStatus, err := InitMemberChangeStatus(configManager, log)
    if err != nil {
        // TODO add log
        return nil, err
    }

    return &LocalHSM{
        StdHSM:             hsm.NewStdHSM(HSMTypeRaft, top, initial),
        dispatchChan:       make(chan hsm.Event, 1),
        selfDispatchChan:   make(chan hsm.Event, 1),
        group:              sync.WaitGroup{},
        localAddr:          localAddr,
        configManager:      configManager,
        stateMachine:       stateMachine,
        log:                log,
        snapshotManager:    snapshotManager,
        Notifier:           NewNotifier(),
        Logger:             logger,
        memberChangeStatus: memberChangeStatus,
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

func (self *LocalHSM) GetMemberChangeStatus() MemberChangeStatusType {
    self.memberChangeStatusLock.RLock()
    defer self.memberChangeStatusLock.RUnlock()
    return self.memberChangeStatus
}

func (self *LocalHSM) SetMemberChangeStatus(status MemberChangeStatusType) {
    self.memberChangeStatusLock.Lock()
    defer self.memberChangeStatusLock.Unlock()
    self.memberChangeStatus = status
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

func (self *LocalHSM) CommitLogsUpTo(index uint64) {
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
        if _, err := self.CommitLogAt(i); err != nil {
            // TODO error handling
        }
    }
}

func (self *LocalHSM) CommitLogAt(index uint64) ([]byte, error) {
    logEntry, err := self.log.GetLog(index)
    if err != nil {
        // TODO add log
        // TODO change panic?
        return nil, err
    }
    return self.CommitLog(logEntry), nil
}

func (self *LocalHSM) CommitLog(logEntry *persist.LogEntry) []byte {
    switch logEntry.Type {
    case persist.LogCommand:
        // TODO add impl
        return self.applyLog(logEntry)
    case persist.LogNoop:
        // just ignore

        /* TODO add other types */
    case persist.LogMemberChange:
        // TODO add impl
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

func InitMemberChangeStatus(
    configManager persist.ConfigManager,
    log persist.Log) (MemberChangeStatusType, error) {

    // setup member change status according to the recent config
    if conf, err := configManager.LastConfig(); err != nil {
        return MemberChangeStatusNotSet, err
    }

    committedIndex, err := log.CommittedIndex()
    if err != nil {
        return MemberChangeStatusNotSet, err
    }
    metas, err := ListAfter(committedIndex)
    if err != nil {
        return MemberChangeStatusNotSet, err
    }
    if len(metas) == 0 {
        return MemberChangeStatusNotSet, errors.New(fmt.Sprintf(
            "no config after committed index %d", committedIndex))
    }
    length := len(metas)
    if length > 2 {
        return MemberChangeStatusNotSet, errors.New(fmt.Sprintf(
            "%d configs after committed index %d", length, committedIndex))
    }
    if length == 1 {
        conf := metas[0].Conf
        if IsNormalConfig(conf) {
            return NotInMemeberChange, nil
        } else if IsOldNewConfig(conf) {
            return OldNewConfigCommitted, nil
        }
    } else { // length == 2
        prevConf = metas[0].Conf
        nextConf = metas[1].Conf
        if IsNormalConfig(prevConf) && IsOldNewConfig(nextConf) {
            return OldNewConfigSeen, nil
        } else if IsOldNewConfig(prevConf) && IsNewConfig(nextConf) {
            return NewConfigSeen, nil
        } else if IsNewConfig(prevConf) && IsNormalConfig(nextConf) {
            return NotInMemeberChange, nil
        }
    }
    return MemberChangeStatusNotSet, errors.New(fmt.Sprintf(
        "corrupted config after committed index %d", committedIndex))
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
    memberChangeState := NewLeaderMemberChangeState(leaderState, logger)
    NewLeaderMemberChangePhase1State(memberChangeState, logger)
    NewLeaderMemberChangePhase2State(memberChangeState, logger)
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
