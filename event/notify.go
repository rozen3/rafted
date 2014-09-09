package event

import (
    hsm "github.com/hhkbp2/go-hsm"
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

const (
    EventNotifyBegin hsm.EventType = EventClientUser + 1 + iota
    EventNotifyHeartbeatTimeout
    EventNotifyElectionTimeout
    EventNotifyElectionTimeoutThreshold
    EventNotifyStateChange
    EventNotifyLeaderChange
    EventNotifyTermChange
    EventNotifyCommit
    EventNotifyApply
    EventNotifyMemberChange
    EventNotifyPersistError
    EventNotifyEnd
)

func NotifyTypeString(event hsm.EventType) string {
    switch event {
    case EventNotifyHeartbeatTimeout:
        return "HeartbeatTimeoutNotify"
    case EventNotifyElectionTimeout:
        return "ElectionTimeoutNotify"
    case EventNotifyElectionTimeoutThreshold:
        return "ElectionTimeoutThresholdNotify"
    case EventNotifyStateChange:
        return "StateChangeNotify"
    case EventNotifyLeaderChange:
        return "LeaderChangeNotify"
    case EventNotifyTermChange:
        return "TermChangeNotify"
    case EventNotifyCommit:
        return "CommitNotify"
    case EventNotifyApply:
        return "ApplyNotify"
    case EventNotifyMemberChange:
        return "MemberChangeNotify"
    case EventNotifyPersistError:
        return "PersistErrorNotify"
    default:
        return "unknown notify"
    }
}

type RaftStateType uint8

const (
    RaftStateUnknown RaftStateType = iota
    RaftStateFollower
    RaftStateCandidate
    RaftStateLeader
)

func (state RaftStateType) String() string {
    switch state {
    case RaftStateUnknown:
        return "RaftStateUnknown"
    case RaftStateFollower:
        return "RaftStateFollower"
    case RaftStateCandidate:
        return "RaftStateCandidate"
    case RaftStateLeader:
        return "RaftStateLeader"
    default:
        return "unknown state"
    }
}

func IsNotifyEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventNotifyBegin, EventNotifyEnd)
}

// The general interface for all notify events.
type NotifyEvent interface {
    hsm.Event
}

// NotifyHeartbeatTimeoutEvent is an event to notify heartbeat timeout.
type NotifyHeartbeatTimeoutEvent struct {
    *hsm.StdEvent
    LastHeartbeatTime time.Time
    Timeout           time.Duration
}

func NewNotifyHeartbeatTimeoutEvent(
    lastHeartbeatTime time.Time,
    timeout time.Duration) *NotifyHeartbeatTimeoutEvent {

    return &NotifyHeartbeatTimeoutEvent{
        StdEvent:          hsm.NewStdEvent(EventNotifyHeartbeatTimeout),
        LastHeartbeatTime: lastHeartbeatTime,
        Timeout:           timeout,
    }
}

// NotifyElectionTimeoutEvent is an event to notify election timeout.
type NotifyElectionTimeoutEvent struct {
    *hsm.StdEvent
    LastElectionTime time.Time
    Timeout          time.Duration
}

func NewNotifyElectionTimeoutEvent(
    lastElectionTime time.Time,
    timeout time.Duration) *NotifyElectionTimeoutEvent {

    return &NotifyElectionTimeoutEvent{
        StdEvent:         hsm.NewStdEvent(EventNotifyElectionTimeout),
        LastElectionTime: lastElectionTime,
        Timeout:          timeout,
    }
}

// NotifyElectionTimeoutThresholdEvent is an event to notify when
// election timeout is about to happen in follower state.
type NotifyElectionTimeoutThresholdEvent struct {
    *hsm.StdEvent
    LastContactTime time.Time
    Timeout         time.Duration
}

func NewNotifyElectionTimeoutThresholdEvent(
    lastContactTime time.Time,
    timeout time.Duration) *NotifyElectionTimeoutThresholdEvent {

    return &NotifyElectionTimeoutThresholdEvent{
        StdEvent:        hsm.NewStdEvent(EventNotifyElectionTimeoutThreshold),
        LastContactTime: lastContactTime,
        Timeout:         timeout,
    }
}

// NotifyElectionTimeoutEvent is an event to notify election timeout.
type NotifyStateChangeEvent struct {
    *hsm.StdEvent
    OldState RaftStateType
    NewState RaftStateType
}

func NewNotifyStateChangeEvent(
    oldState, newState RaftStateType) *NotifyStateChangeEvent {

    return &NotifyStateChangeEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyStateChange),
        OldState: oldState,
        NewState: newState,
    }
}

// NotifyLeaderChangeEvent is an event to notify that leader has changed.
type NotifyLeaderChangeEvent struct {
    *hsm.StdEvent
    NewLeader ps.ServerAddr
}

func NewNotifyLeaderChangeEvent(
    newLeader ps.ServerAddr) *NotifyLeaderChangeEvent {

    return &NotifyLeaderChangeEvent{
        StdEvent:  hsm.NewStdEvent(EventNotifyLeaderChange),
        NewLeader: newLeader,
    }
}

// NotifyTermChangeEvent is an event to notify term has changed.
type NotifyTermChangeEvent struct {
    *hsm.StdEvent
    OldTerm uint64
    NewTerm uint64
}

func NewNotifyTermChangeEvent(oldTerm, newTerm uint64) *NotifyTermChangeEvent {
    return &NotifyTermChangeEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyTermChange),
        OldTerm:  oldTerm,
        NewTerm:  newTerm,
    }
}

// NotifyCommitEvent is an event to notify a log entry has committed.
type NotifyCommitEvent struct {
    *hsm.StdEvent
    Term     uint64
    LogIndex uint64
}

func NewNotifyCommitEvent(term, logIndex uint64) *NotifyCommitEvent {
    return &NotifyCommitEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyCommit),
        Term:     term,
        LogIndex: logIndex,
    }
}

// NotifyApplyEvent is an event to notify a log entry has applied.
type NotifyApplyEvent struct {
    *hsm.StdEvent
    Term     uint64
    LogIndex uint64
}

func NewNotifyApplyEvent(term, logIndex uint64) *NotifyApplyEvent {
    return &NotifyApplyEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyApply),
        Term:     term,
        LogIndex: logIndex,
    }
}

// NotifyMemberChangeEvent is an event to notify a member change procedure
// has happened in cluster.
type NotifyMemberChangeEvent struct {
    *hsm.StdEvent
    OldServers []ps.ServerAddr
    NewServers []ps.ServerAddr
}

func NewNotifyMemberChangeEvent(
    oldServers, newServers []ps.ServerAddr) *NotifyMemberChangeEvent {

    return &NotifyMemberChangeEvent{
        StdEvent:   hsm.NewStdEvent(EventNotifyMemberChange),
        OldServers: oldServers,
        NewServers: newServers,
    }
}

// NotifyPersistErrorEvent is an event to notify a persist action has failed.
// Probably it's a hard disk failure.
type NotifyPersistErrorEvent struct {
    *hsm.StdEvent
    Error error
}

func NewNotifyPersistErrorEvent(err error) *NotifyPersistErrorEvent {
    return &NotifyPersistErrorEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyPersistError),
        Error:    err,
    }
}
