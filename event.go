package rafted

import hsm "github.com/hhkbp2/go-hsm"

const (
    EventTerm      = hsm.EventUser + 1
    EventRaftBegin = 100 + iota
    EventRequestVoteRequest
    EventRequestVoteResponse
    EventAppendEntriesRequest
    EventAppendEntriesResponse
    EventPrepareInstallSnapshotRequest
    EventPrepareInstallSnapshotResponse
    EventInstallSnapshotRequest
    EventInstallSnapshotResponse
    EventRaftEnd
    EventTimeoutBegin = 1000 + iota
    EventTimeoutHeartBeat
    EventTimeoutElection
    EventTimeoutEnd
    EventClientBegin = 2000 + iota
    EventReadRequest
    EventReadResponse
    EventWriteRequest
    EventWriteResponse
    EventRedirectResponse
    EventLeaderUnknownResponse
    EventClientEnd
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
    return IsEventBetween(eventType, EventClientBegin, EventClientEnd)
}

type RequestEvent struct {
    *hsm.StdEvent
    ResultChan chan interface{}
}

func NewRequestEvent(eventType hsm.EventType, resultChan chan interface{}) *RequestEvent {
    return &RequestEvent{hsm.NewStdEvent(eventType), resultChan}
}

type RequestVoteRequestEvent struct {
    *RequestEvent
    // extended fields
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(request *RequestVoteRequest, resultChan chan interface{}) *RequestVoteRequestEvent {
    return &RequestVoteRequestEvent{NewRequestEvent(EventRequestVoteRequest, resultChan), request}
}

type RequestVoteResponseEvent struct {
    *hsm.StdEvent
    Response *RequestVoteResponse
}

func NewRequestVoteResponseEvent(response *RequestVoteResponse) *RequestVoteResponseEvent {
    return &RequestVoteResponseEvent{hsm.NewStdEvent(EventRequestVoteResponse), response}
}

type AppendEntriesReqeustEvent struct {
    *RequestEvent
    // extended fields
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(request *AppendEntriesRequest, resultChan chan interface{}) *AppendEntriesReqeustEvent {
    return &AppendEntriesReqeustEvent{NewRequestEvent(EventAppendEntriesRequest, resultChan), request}
}

type AppendEntriesResponseEvent struct {
    *hsm.StdEvent
    Response *AppendEntriesResponse
}

func NewAppendEntriesResponseEvent(response *AppendEntriesResponse) *AppendEntriesResponseEvent {
    return &AppendEntriesResponseEvent{hsm.NewStdEvent(EventAppendEntriesResponse), response}
}

type PrepareInstallSnapshotRequestEvent struct {
    *RequestEvent
    // extended fields
    Request *PrepareInstallSnapshotRequest
}

func NewPrepareInstallSnapshotRequestEvent(request *PrepareInstallSnapshotRequest, resultChan chan interface{}) *PrepareInstallSnapshotRequestEvent {
    return &PrepareInstallSnapshotRequestEvent{NewRequestEvent(EventPrepareInstallSnapshotRequest, resultChan), request}
}

type PrepareInstallSnapshotResponseEvent struct {
    *hsm.StdEvent
    Response *PrepareInstallSnapshotResponse
}

func NewPrepareInstallSnapshotResponseEvent(response *PrepareInstallSnapshotResponse) *PrepareInstallSnapshotResponseEvent {
    return &PrepareInstallSnapshotResponseEvent{hsm.NewStdEvent(EventPrepareInstallSnapshotResponse), response}
}

type InstallSnapshotRequestEvent struct {
    *RequestEvent
    // extended fields
    Request *InstallSnapshotRequest
}

func NewInstallSnapshotRequestEvent(request *InstallSnapshotRequest, resultChan chan interface{}) *InstallSnapshotRequestEvent {
    return &InstallSnapshotRequestEvent{NewRequestEvent(EventInstallSnapshotRequest, resultChan), request}
}

type InstallSnapshotResponseEvent struct {
    *hsm.StdEvent
    Response *PrepareInstallSnapshotResponse
}

func NewInstallSnapshotResponseEvent(response *InstallSnapshotResponse) *InstallSnapshotRequestEvent {
    return &InstallSnapshotResponseEvent{hsm.NewStdEvent(EventInstallSnapshotResponse), response}
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

type ReadRequestEvent struct {
    *RequestEvent
    Request *ReadRequest
}

func NewReadRequstEvent(request *ReadRequest, resultChan chan interface{}) *ReadRequestEvent {
    return &ReadRequestEvent{NewRequestEvent(EventReadRequest, resultChan), request}
}

type ReadResponseEvent struct {
    *hsm.StdEvent
    Response *ReadResponse
}

func NewReadResponseEvent(response *ReadResponse) *ReadResponseEvent {
    return &ReadResponseEvent{hsm.NewStdEvent(EventReadResponse), response}
}

type RedirectResponseEvent struct {
    *hsm.StdEvent
    Response *RedirectResponse
}

func NewRedirectResponseEvent(response *RedirectResponse) *RedirectResponseEvent {
    return &RedirectResponseEvent{hsm.NewStdEvent(EventRedirectResponse), response}
}

type LeaderUnknownResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderUnknownResponse
}

func NewLeaderUnknownResponseEvent(response *LeaderUnknownResponse) *LeaderUnknownResponseEvent {
    return &LeaderUnknownResponseEvent{hsm.NewStdEvent(EventLeaderUnknownResponse), response}
}
