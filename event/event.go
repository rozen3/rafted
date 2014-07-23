package event

import hsm "github.com/hhkbp2/go-hsm"

const (
    EventTerm hsm.EventType = hsm.EventUser + 100 + iota
    EventRaftBegin
    EventAppendEntriesRequest
    EventAppendEntriesResponse
    EventRequestVoteRequest
    EventRequestVoteResponse
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

func IsRaftRequest(eventType hsm.EventType) bool {
    switch eventType {
    case EventAppendEntriesRequest:
    case EventRequestVoteRequest:
    case EventInstallSnapshotRequest:
    default:
        return false
    }
    return true
}

func IsTimeoutEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventTimeoutBegin, EventTimeoutEnd)
}

func IsClientEvent(eventType hsm.EventType) bool {
    return (eventType >= EventClientUser)
}

type RaftEvent interface {
    hsm.Event
    Message() interface{}
}

type ResponsiveEvent interface {
    SendResponse(RaftEvent)
    RecvResponse() RaftEvent
}

type RequestEvent interface {
    RaftEvent
    ResponsiveEvent
}

type RequestEventHead struct {
    *hsm.StdEvent
    resultChan chan RaftEvent
}

func (self *RequestEventHead) SendResponse(event RaftEvent) {
    self.resultChan <- event
}

func (self *RequestEventHead) RecvResponse() RaftEvent {
    response := <-self.resultChan
    return response
}

func NewRequestEventHead(eventType hsm.EventType) *RequestEventHead {
    return &RequestEventHead{
        hsm.NewStdEvent(eventType),
        make(chan RaftEvent, 1),
    }
}

type AppendEntriesReqeustEvent struct {
    *RequestEventHead
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(
    request *AppendEntriesRequest) *AppendEntriesReqeustEvent {

    return &AppendEntriesReqeustEvent{
        NewRequestEventHead(EventAppendEntriesRequest),
        request,
    }
}
func (self *AppendEntriesReqeustEvent) Message() interface{} {
    return self.Request
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

func (self *AppendEntriesResponseEvent) Message() interface{} {
    return self.Response
}

type RequestVoteRequestEvent struct {
    *RequestEventHead
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(
    request *RequestVoteRequest) *RequestVoteRequestEvent {

    return &RequestVoteRequestEvent{
        NewRequestEventHead(EventRequestVoteRequest),
        request,
    }
}

func (self *RequestVoteRequestEvent) Message() interface{} {
    return self.Request
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

func (self *RequestVoteResponseEvent) Message() interface{} {
    return self.Response
}

type InstallSnapshotRequestEvent struct {
    *RequestEventHead
    Request *InstallSnapshotRequest
}

func NewInstallSnapshotRequestEvent(
    request *InstallSnapshotRequest) *InstallSnapshotRequestEvent {
    return &InstallSnapshotRequestEvent{
        NewRequestEventHead(EventInstallSnapshotRequest),
        request,
    }
}

func (self *InstallSnapshotRequestEvent) Message() interface{} {
    return self.Request
}

type InstallSnapshotResponseEvent struct {
    *hsm.StdEvent
    Response *InstallSnapshotResponse
}

func NewInstallSnapshotResponseEvent(
    response *InstallSnapshotResponse) *InstallSnapshotResponseEvent {

    return &InstallSnapshotResponseEvent{
        hsm.NewStdEvent(EventInstallSnapshotResponse),
        response,
    }
}

func (self *InstallSnapshotResponseEvent) Message() interface{} {
    return self.Response
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
        hsm.NewStdEvent(EventLeaderRedirectResponse),
        response,
    }
}

func (self *LeaderRedirectResponseEvent) Message() interface{} {
    return self.Response
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

func (self *LeaderUnknownResponseEvent) Message() interface{} {
    return self.Response
}
