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
    ResultChan chan hsm.Event
}

func NewRequestEvent(eventType hsm.EventType) *RequestEvent {
    return &RequestEvent{hsm.NewStdEvent(eventType), make(chan hsm.Event)}
}

type RequestVoteRequestEvent struct {
    *RequestEvent
    // extended fields
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(request *RequestVoteRequest) *RequestVoteRequestEvent {
    return &RequestVoteRequestEvent{NewRequestEvent(EventRequestVoteRequest), request}
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

func NewAppendEntriesRequestEvent(request *AppendEntriesRequest) *AppendEntriesReqeustEvent {
    return &AppendEntriesReqeustEvent{NewRequestEvent(EventAppendEntriesRequest), request}
}

type AppendEntriesResponseEvent struct {
    *hsm.StdEvent
    Response *AppendEntriesResponse
}

func NewAppendEntriesResponseEvent(response *AppendEntriesResponse) *AppendEntriesResponseEvent {
    return &AppendEntriesResponseEvent{hsm.NewStdEvent(EventAppendEntriesResponse), response}
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

func NewReadRequstEvent(request *ReadRequest) *ReadRequestEvent {
    return &ReadRequestEvent{NewRequestEvent(EventReadRequest), request}
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
