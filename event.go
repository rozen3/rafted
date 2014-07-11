package rafted

import hsm "github.com/hhkbp2/go-hsm"

const (
    EventTerm hsm.EventType = hsm.EventUser + 100 + iota
    EventRaftBegin
    EventAppendEntriesRequest
    EventAppendEntriesResponse
    EventRequestVoteRequest
    EventRequestVoteResponse
    EventPrepareInstallSnapshotRequest
    EventPrepareInstallSnapshotResponse
    EventInstallSnapshotRequest
    EventInstallSnapshotResponse
    EventRaftEnd
    EventTimeoutBegin
    EventTimeoutHeartBeat
    EventTimeoutElection
    EventTimeoutEnd
    EventLeaderRedirectResponse
    EventLeaderUnknownResponse
    EventClientUser = hsm.EventUser + 1000 + iota
)

func IsEventBetween(eventType, beginEvent, endEvent hsm.EventType) bool {
    if (eventType > beginEvent) && (eventType < endEvent) {
        return true
    }
    return false
}

func IsRaftEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventRaftBegin, EventRaftEnd)
}

func IsTimeoutEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventTimeoutBegin, EventTimeoutEnd)
}

func IsClientEvent(eventType hsm.EventType) bool {
    return (eventType >= EventClientUser)
}

type ResponsiveEvent interface {
    Response(hsm.Event)
}

type RequestEvent struct {
    *hsm.StdEvent
    ResultChan chan hsm.Event
}

func (self *RequestEvent) Response(event hsm.Event) {
    self.ResultChan <- event
}

func NewRequestEvent(
    eventType hsm.EventType,
    resultChan chan hsm.Event) *RequestEvent {
    return &RequestEvent{hsm.NewStdEvent(eventType), resultChan}
}

type AppendEntriesReqeustEvent struct {
    *RequestEvent
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(
    request *AppendEntriesRequest,
    resultChan chan hsm.Event) *AppendEntriesReqeustEvent {
    return &AppendEntriesReqeustEvent{
        NewRequestEvent(EventAppendEntriesRequest, resultChan),
        request}
}

type AppendEntriesResponseEvent struct {
    *hsm.StdEvent
    Response *AppendEntriesResponse
}

func NewAppendEntriesResponseEvent(
    response *AppendEntriesResponse) *AppendEntriesResponseEvent {
    return &AppendEntriesResponseEvent{
        hsm.NewStdEvent(EventAppendEntriesResponse),
        response,
    }
}

type RequestVoteRequestEvent struct {
    *RequestEvent
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(
    request *RequestVoteRequest,
    resultChan chan hsm.Event) *RequestVoteRequestEvent {
    return &RequestVoteRequestEvent{
        NewRequestEvent(EventRequestVoteRequest, resultChan),
        request,
    }
}

type RequestVoteResponseEvent struct {
    *hsm.StdEvent
    Response *RequestVoteResponse
}

func NewRequestVoteResponseEvent(
    response *RequestVoteResponse) *RequestVoteResponseEvent {
    return &RequestVoteResponseEvent{
        hsm.NewStdEvent(EventRequestVoteResponse),
        response,
    }
}

type PrepareInstallSnapshotRequestEvent struct {
    *RequestEvent
    Request *PrepareInstallSnapshotRequest
}

func NewPrepareInstallSnapshotRequestEvent(
    request *PrepareInstallSnapshotRequest,
    resultChan chan hsm.Event) *PrepareInstallSnapshotRequestEvent {
    return &PrepareInstallSnapshotRequestEvent{
        NewRequestEvent(EventPrepareInstallSnapshotRequest, resultChan),
        request,
    }
}

type PrepareInstallSnapshotResponseEvent struct {
    *hsm.StdEvent
    Response *PrepareInstallSnapshotResponse
}

func NewPrepareInstallSnapshotResponseEvent(
    response *PrepareInstallSnapshotResponse) *PrepareInstallSnapshotResponseEvent {
    return &PrepareInstallSnapshotResponseEvent{
        hsm.NewStdEvent(EventPrepareInstallSnapshotResponse),
        response,
    }
}

type InstallSnapshotRequestEvent struct {
    *RequestEvent
    // extended fields
    Request *InstallSnapshotRequest
}

func NewInstallSnapshotRequestEvent(
    request *InstallSnapshotRequest,
    resultChan chan hsm.Event) *InstallSnapshotRequestEvent {
    return &InstallSnapshotRequestEvent{
        NewRequestEvent(EventInstallSnapshotRequest, resultChan),
        request,
    }
}

type InstallSnapshotResponseEvent struct {
    *hsm.StdEvent
    Response *PrepareInstallSnapshotResponse
}

func NewInstallSnapshotResponseEvent(
    response *InstallSnapshotResponse) *InstallSnapshotRequestEvent {
    return &InstallSnapshotResponseEvent{
        hsm.NewStdEvent(EventInstallSnapshotResponse),
        response,
    }
}

type HeartbeatTimeoutEvent struct {
    *hsm.StdEvent
}

func NewHeartbeatTimeoutEvent() *HeartbeatTimeoutEvent {
    return &HeartbeatTimeoutEvent{hsm.NewStdEvent(EventTimeoutHeartBeat)}
}

type ElectionTimeoutEvent struct {
    *hsm.StdEvent
}

func NewElectionTimeoutEvent() *ElectionTimeoutEvent {
    return &ElectionTimeoutEvent{hsm.NewStdEvent(EventTimeoutElection)}
}

type LeaderRedirectResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderRedirectResponse
}

func NewLeaderRedirectResponseEvent(
    response *LeaderRedirectResponse) *LeaderRedirectResponseEvent {
    return &LeaderRedirectResponseEvent{
        hsm.NewStdEvent(EventRedirectResponse),
        response,
    }
}

type LeaderUnknownResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderUnknownResponse
}

func NewLeaderUnknownResponseEvent(
    response *LeaderUnknownResponse) *LeaderUnknownResponseEvent {
    return &LeaderUnknownResponseEvent{
        hsm.NewStdEvent(EventLeaderUnknownResponse),
        response,
    }
}
