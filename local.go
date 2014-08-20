package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "sync"
    "sync/atomic"
    "time"
)

const (
    HSMTypeLocal hsm.HSMType = hsm.HSMTypeStd + 1 + iota
    HSMTypePeer
    HSMTypeLeaderMemberChange
)

type MemberChangeStatusType uint8

const (
    MemberChangeStatusNotSet MemberChangeStatusType = iota
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
    selfDispatchChan *ReliableEventChannel
    group            sync.WaitGroup

    // the current term
    currentTerm uint64

    // the local addr
    localAddr     ps.ServerAddr
    localAddrLock sync.RWMutex

    // CandidateId that received vote in current term(or nil if none)
    votedFor     ps.ServerAddr
    votedForLock sync.RWMutex
    // leader infos
    leader     ps.ServerAddr
    leaderLock sync.RWMutex

    // member change infos
    memberChangeStatus     MemberChangeStatusType
    memberChangeStatusLock sync.RWMutex

    // configuration
    configManager ps.ConfigManager

    // state machine is exclusively used only in here
    stateMachine ps.StateMachine

    // log entries
    log ps.Log

    // snapshot
    snapshotManager ps.SnapshotManager

    // peers
    peerManager *PeerManager

    // notifier
    notifier *Notifier

    logging.Logger
}

func NewLocalHSM(
    top hsm.State,
    initial hsm.State,
    localAddr ps.ServerAddr,
    configManager ps.ConfigManager,
    stateMachine ps.StateMachine,
    log ps.Log,
    snapshotManager ps.SnapshotManager,
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
        StdHSM:             hsm.NewStdHSM(HSMTypeLocal, top, initial),
        dispatchChan:       make(chan hsm.Event, 1),
        selfDispatchChan:   NewReliableEventChannel(),
        group:              sync.WaitGroup{},
        localAddr:          localAddr,
        configManager:      configManager,
        stateMachine:       stateMachine,
        log:                log,
        snapshotManager:    snapshotManager,
        notifier:           NewNotifier(),
        Logger:             logger,
        memberChangeStatus: memberChangeStatus,
    }, nil
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
    priorityChan := self.selfDispatchChan.GetOutChan()
    for {
        // Event in selfDispatchChan has higher priority to be processed
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
    self.selfDispatchChan.Send(event)
}

func (self *LocalHSM) Terminate() {
    self.SelfDispatch(hsm.NewStdEvent(ev.EventTerm))
    self.group.Wait()
    self.selfDispatchChan.Close()
}

func (self *LocalHSM) GetCurrentTerm() uint64 {
    return atomic.LoadUint64(&self.currentTerm)
}

func (self *LocalHSM) SetCurrentTerm(term uint64) {
    atomic.StoreUint64(&self.currentTerm, term)
}

func (self *LocalHSM) SetCurrentTermWithNotify(term uint64) {
    oldTerm := self.GetCurrentTerm()
    self.SetCurrentTerm(term)
    self.Notifier().Notify(ev.NewNotifyTermChangeEvent(oldTerm, term))
}

func (self *LocalHSM) GetLocalAddr() ps.ServerAddr {
    self.localAddrLock.RLock()
    defer self.localAddrLock.RUnlock()
    return self.localAddr
}

func (self *LocalHSM) SetLocalAddr(addr ps.ServerAddr) {
    self.localAddrLock.Lock()
    defer self.localAddrLock.Unlock()
    self.localAddr = addr
}

func (self *LocalHSM) GetVotedFor() ps.ServerAddr {
    self.votedForLock.RLock()
    defer self.votedForLock.RUnlock()
    return self.votedFor
}

func (self *LocalHSM) SetVotedFor(votedFor ps.ServerAddr) {
    self.votedForLock.Lock()
    defer self.votedForLock.Unlock()
    self.votedFor = votedFor
}

func (self *LocalHSM) GetLeader() ps.ServerAddr {
    self.leaderLock.RLock()
    defer self.leaderLock.RUnlock()
    return self.leader
}

func (self *LocalHSM) SetLeader(leader ps.ServerAddr) {
    self.leaderLock.Lock()
    defer self.leaderLock.Unlock()
    self.leader = leader
}

func (self *LocalHSM) SetLeaderWithNotify(leader ps.ServerAddr) {
    self.SetLeader(leader)
    self.Notifier().Notify(ev.NewNotifyLeaderChangeEvent(leader))
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

func (self *LocalHSM) ConfigManager() ps.ConfigManager {
    return self.configManager
}

func (self *LocalHSM) Log() ps.Log {
    return self.log
}

func (self *LocalHSM) SnapshotManager() ps.SnapshotManager {
    return self.snapshotManager
}

func (self *LocalHSM) PeerManager() *PeerManager {
    return self.peerManager
}

func (self *LocalHSM) SetPeerManager(peerManager *PeerManager) {
    self.peerManager = peerManager
}

func (self *LocalHSM) Notifier() *Notifier {
    return self.notifier
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

func (self *LocalHSM) CommitLog(logEntry *ps.LogEntry) []byte {
    switch logEntry.Type {
    case ps.LogCommand:
        // TODO add impl
        return self.applyLog(logEntry)
    case ps.LogNoop:
        // just ignore

        /* TODO add other types */
    case ps.LogMemberChange:
        // nothing to do for member change
        return nil
    default:
        // unknown log entry type
        // TODO add log
    }
    return nil
}

func (self *LocalHSM) applyLog(logEntry *ps.LogEntry) []byte {
    result := self.stateMachine.Apply(logEntry.Data)
    self.log.StoreLastAppliedIndex(logEntry.Index)
    return result
}

func InitMemberChangeStatus(
    configManager ps.ConfigManager,
    log ps.Log) (MemberChangeStatusType, error) {

    // setup member change status according to the recent config
    committedIndex, err := log.CommittedIndex()
    if err != nil {
        return MemberChangeStatusNotSet, err
    }
    metas, err := configManager.ListAfter(committedIndex)
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
        if ps.IsNormalConfig(conf) {
            return NotInMemeberChange, nil
        } else if ps.IsOldNewConfig(conf) {
            return OldNewConfigCommitted, nil
        }
    } else { // length == 2
        prevConf := metas[0].Conf
        nextConf := metas[1].Conf
        if ps.IsNormalConfig(prevConf) && ps.IsOldNewConfig(nextConf) {
            return OldNewConfigSeen, nil
        } else if ps.IsOldNewConfig(prevConf) && ps.IsNewConfig(nextConf) {
            return NewConfigSeen, nil
        } else if ps.IsNewConfig(prevConf) && ps.IsNormalConfig(nextConf) {
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
    electionTimeoutThresholdPersent float32,
    localAddr ps.ServerAddr,
    configManager ps.ConfigManager,
    stateMachine ps.StateMachine,
    log ps.Log,
    snapshotManager ps.SnapshotManager,
    logger logging.Logger) (*Local, error) {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateLocalID)
    localState := NewLocalState(top, logger)
    followerState := NewFollowerState(
        localState, heartbeatTimeout, electionTimeoutThresholdPersent, logger)
    NewSnapshotRecoveryState(followerState, logger)
    followerMemberChangeState := NewFollowerMemberChangeState(followerState, logger)
    NewFollowerOldNewConfigSeenState(followerMemberChangeState, logger)
    NewFollowerOldNewConfigCommittedState(followerState, logger)
    NewFollowerNewConfigSeenState(followerState, logger)
    needPeersState := NewNeedPeersState(localState, logger)
    NewCandidateState(needPeersState, electionTimeout, logger)
    leaderState := NewLeaderState(needPeersState, logger)
    NewUnsyncState(leaderState, logger)
    NewSyncState(leaderState, logger)
    localHSM, err := NewLocalHSM(
        top,
        initial,
        localAddr,
        configManager,
        stateMachine,
        log,
        snapshotManager,
        logger)
    if err != nil {
        return nil, err
    }
    localHSM.Init()
    return &Local{localHSM}, nil
}
