package event

import (
    hsm "github.com/hhkbp2/go-hsm"
    "net"
    "time"
)

const (
    EventNotifyBegin hsm.EventType = EventClientUser + 1 + iota
    EventNotifyHeartbeatTimeout
    EventNotifyElectionTimeout
    EventNotifyStateChange
    EventNotifyLeaderChange
    EventNotifyTermChange
    EventNotifyCommit
    EventNotifyMemberChange
    EventNotifyPersistError
    EventNotifyEnd
)

func IsNotifyEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventNotifyBegin, EventNotifyEnd)
}

type NotifyEvent interface {
    hsm.Event
}

type NotifyHeartbeatTimeoutEvent struct {
    *hsm.StdEvent
    LastHeartbeatTime time.Time
}

func NewNotifyHeartbeatTimeoutEvent(
    lastHeartbeatTime time.Time) *NotifyHeartbeatTimeoutEvent {

    return &NotifyHeartbeatTimeoutEvent{
        hsm.NewStdEvent(EventNotifyHeartbeatTimeout),
        lastHeartbeatTime,
    }
}

type NotifyElectionTimeoutEvent struct {
    *hsm.StdEvent
    LastElectionTime time.Time
}

func NewNotifyElectionTimeoutEvent(
    lastElectionTime time.Time) *NotifyElectionTimeoutEvent {

    return &NotifyElectionTimeoutEvent{
        hsm.NewStdEvent(EventNotifyElectionTimeout),
        lastElectionTime,
    }
}

type NotifyStateChangeEvent struct {
    *hsm.StdEvent
    OldState string
    NewState string
}

func NewNotifyStateChangeEvent(
    oldState, newState string) *NotifyStateChangeEvent {

    return &NotifyStateChangeEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyStateChange),
        OldState: oldState,
        NewState: newState,
    }
}

type NotifyLeaderChangeEvent struct {
    *hsm.StdEvent
    OldLeader net.Addr
    NewLeader net.Addr
}

func NewNotifyLeaderChangeEvent(
    oldLeader, newLeader net.Addr) *NotifyLeaderChangeEvent {

    return &NotifyLeaderChangeEvent{
        StdEvent:  hsm.NewStdEvent(EventNotifyLeaderChange),
        OldLeader: oldLeader,
        NewLeader: newLeader,
    }
}

type NotifyTermChangeEvent struct {
    *hsm.StdEvent
    OldTerm uint64
    NewTerm uint64
}

func NewNotifyTermChangeEvent(
    oldTerm, newTerm uint64) *NotifyTermChangeEvent {

    return &NotifyTermChangeEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyTermChange),
        OldTerm:  oldTerm,
        NewTerm:  newTerm,
    }
}

type NotifyAddPeerEvent struct {
    *hsm.StdEvent
    PeerAdded net.Addr
}

func NewNotifyAddPeerEvent(
    peerAdded net.Addr) *NotifyAddPeerEvent {

    return &NotifyAddPeerEvent{
        StdEvent:  hsm.NewStdEvent(EventNotifyAddPeer),
        PeerAdded: peerAdded,
    }
}

type NotifyRemovePeerEvent struct {
    *hsm.StdEvent
    PeerRemoved net.Addr
}

func NewNotifyRemovePeerEvent(
    peerRemoved net.Addr) *NotifyRemovePeerEvent {

    return &NotifyRemovePeerEvent{
        StdEvent:    hsm.NewStdEvent(EventNotifyRemovePeer),
        PeerRemoved: peerRemoved,
    }
}

type NotifyCommitEvent struct {
    *hsm.StdEvent
    Term     uint64
    LogIndex uint64
}

func NewNotifyCommitEvent(
    term, logIndex uint64) *NotifyCommitEvent {

    return &NotifyCommitEvent{
        StdEvent: hsm.NewStdEvent(EventNotifyCommit),
        Term:     term,
        LogIndex: logIndex,
    }
}
