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
    EventTimeoutHeartbeat
    EventTimeoutElection
    EventTimeoutEnd
    EventInternalBegin
    EventAbortSnapshotRecovery
    EventStepdown
    EventPeerReplicateLog
    EventPeerActivate
    EventPeerDeactivate
    EventPeerEnterLeader
    EventPeerEnterSnapshotMode
    EventPeerAbortSnapshotMode
    EventInternalEnd
    EventClientRequestBegin
    EventClientWriteRequest
    EventClientReadOnlyRequest
    EventClientMemberChangeRequest
    EventClientRequestEnd
    EventClientResponse
    EventLeaderRedirectResponse
    EventLeaderUnknownResponse
    EventLeaderUnsyncResponse
    EventClientUser = hsm.EventUser + 1000 + iota
)

func PrintEvent(event hsm.Event) string {
    switch event.Type() {
    case hsm.EventInit:
        return "InitEvent"
    case hsm.EventEntry:
        return "EntryEvent"
    case hsm.EventExit:
        return "ExitEvent"
    case EventTerm:
        return "TermEvent"
    case EventAppendEntriesRequest:
        return "AppendEntriesRequestEvent"
    case EventAppendEntriesResponse:
        return "AppendEntriesResponseEvent"
    case EventRequestVoteRequest:
        return "RequestVoteRequestEvent"
    case EventRequestVoteResponse:
        return "RequestVoteResponseEvent"
    case EventInstallSnapshotRequest:
        return "InstallSnapshotRequestEvent"
    case EventInstallSnapshotResponse:
        return "InstallSnapshotResponseEvent"
    case EventTimeoutHeartbeat:
        return "HearbeatTiemoutEvent"
    case EventTimeoutElection:
        return "ElectionTimeoutEvent"
    case EventAbortSnapshotRecovery:
        return "AbortSnapshotRecoveryEvent"
    case EventStepdown:
        return "StepdownEvent"
    case EventPeerReplicateLog:
        return "PeerReplicateLogEvent"
    case EventPeerActivate:
        return "ActivatePeerEvent"
    case EventPeerDeactivate:
        return "DeactivatePeerEvent"
    case EventPeerAbortSnapshotMode:
        return "PeerAbortSnapshotModeEvent"
    case EventClientWriteRequest:
        return "ClientWriteRequestEvent"
    case EventClientReadOnlyRequest:
        return "ClientReadOnlyRequest"
    default:
        return "Unknown Event"
    }
}

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
        fallthrough
    case EventRequestVoteRequest:
        fallthrough
    case EventInstallSnapshotRequest:
        return true
    default:
        return false
    }
}

func IsTimeoutEvent(eventType hsm.EventType) bool {
    return IsEventBetween(eventType, EventTimeoutBegin, EventTimeoutEnd)
}

func IsClientEvent(eventType hsm.EventType) bool {
    return IsEventBetween(
        eventType, EventClientRequestBegin, EventClientRequestEnd)
}

type RaftEvent interface {
    hsm.Event
    Message() interface{}
}

type RaftRequestEvent interface {
    RaftEvent
    SendResponse(RaftEvent)
    RecvResponse() RaftEvent
}

type RaftRequestEventHead struct {
    *hsm.StdEvent
    resultChan chan RaftEvent
}

func (self *RaftRequestEventHead) SendResponse(event RaftEvent) {
    self.resultChan <- event
}

func (self *RaftRequestEventHead) RecvResponse() RaftEvent {
    response := <-self.resultChan
    return response
}

func NewRaftRequestEventHead(eventType hsm.EventType) *RaftRequestEventHead {
    return &RaftRequestEventHead{
        hsm.NewStdEvent(eventType),
        make(chan RaftEvent, 1),
    }
}

type AppendEntriesReqeustEvent struct {
    *RaftRequestEventHead
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(
    request *AppendEntriesRequest) *AppendEntriesReqeustEvent {

    return &AppendEntriesReqeustEvent{
        NewRaftRequestEventHead(EventAppendEntriesRequest),
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
    *RaftRequestEventHead
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(
    request *RequestVoteRequest) *RequestVoteRequestEvent {

    return &RequestVoteRequestEvent{
        NewRaftRequestEventHead(EventRequestVoteRequest),
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
    *RaftRequestEventHead
    Request *InstallSnapshotRequest
}

func NewInstallSnapshotRequestEvent(
    request *InstallSnapshotRequest) *InstallSnapshotRequestEvent {
    return &InstallSnapshotRequestEvent{
        NewRaftRequestEventHead(EventInstallSnapshotRequest),
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
    Message *HeartbeatTimeout
}

func NewHeartbeatTimeoutEvent(
    message *HeartbeatTimeout) *HeartbeatTimeoutEvent {

    return &HeartbeatTimeoutEvent{
        hsm.NewStdEvent(EventTimeoutHeartbeat),
        message,
    }
}

type ElectionTimeoutEvent struct {
    *hsm.StdEvent
}

func NewElectionTimeoutEvent() *ElectionTimeoutEvent {
    return &ElectionTimeoutEvent{hsm.NewStdEvent(EventTimeoutElection)}
}

type ClientEvent interface {
    hsm.Event
}

type ClientRequestEvent interface {
    ClientEvent
    SendResponse(ClientEvent)
    RecvResponse() ClientEvent
}

type ClientRequestEventHead struct {
    *hsm.StdEvent
    ResultChan chan ClientEvent
}

func (self *ClientRequestEventHead) SendResponse(event ClientEvent) {
    self.ResultChan <- event
}

func (self *ClientRequestEventHead) RecvResponse() ClientEvent {
    response := <-self.ResultChan
    return response
}

func NewClientRequestEventHead(
    eventType hsm.EventType) *ClientRequestEventHead {

    return &ClientRequestEventHead{
        hsm.NewStdEvent(eventType),
        make(chan ClientEvent, 1),
    }
}

type ClientWriteRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientWriteRequest
}

func NewClientWriteRequestEvent(
    request *ClientWriteRequest) *ClientWriteRequestEvent {

    return &ClientWriteRequestEvent{
        NewClientRequestEventHead(EventClientWriteRequest),
        request,
    }
}

type ClientReadOnlyRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientReadOnlyRequest
}

func NewClientReadOnlyRequestEvent(
    request *ClientReadOnlyRequest) *ClientReadOnlyRequestEvent {

    return &ClientReadOnlyRequestEvent{
        NewClientRequestEventHead(EventClientReadOnlyRequest),
        request,
    }
}

type ClientMemberChangeRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientMemberChangeRequest
}

func NewClientMemberChangeRequestEvent(
    request *ClientMemberChangeRequest) *ClientMemberChangeRequestEvent {

    return &ClientMemberChangeRequestEvent{
        NewClientRequestEventHead(EventClientMemberChangeRequest),
        request,
    }
}

type ClientResponseEvent struct {
    *hsm.StdEvent
    Response *ClientResponse
}

func NewClientResponseEvent(
    response *ClientResponse) *ClientResponseEvent {

    return &ClientResponseEvent{
        hsm.NewStdEvent(EventClientResponse),
        response,
    }
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

type LeaderUnsyncResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderUnsyncResponse
}

func NewLeaderUnsyncResponseEvent(
    response *LeaderUnsyncResponse) *LeaderUnsyncResponseEvent {

    return &LeaderUnsyncResponseEvent{
        hsm.NewStdEvent(EventLeaderUnsyncResponse),
        response,
    }
}

type AbortSnapshotRecoveryEvent struct {
    *hsm.StdEvent
}

func NewAbortSnapshotRecoveryEvent() *AbortSnapshotRecoveryEvent {
    return &AbortSnapshotRecoveryEvent{
        hsm.NewStdEvent(EventAbortSnapshotRecovery),
    }
}

type StepdownEvent struct {
    *hsm.StdEvent
}

func NewStepdownEvent() *StepdownEvent {
    return &StepdownEvent{
        hsm.NewStdEvent(EventStepdown),
    }
}

type PeerReplicateLogEvent struct {
    *hsm.StdEvent
    Message *PeerReplicateLog
}

func NewPeerReplicateLogEvent(
    message *PeerReplicateLog) *PeerReplicateLogEvent {

    return &PeerReplicateLogEvent{
        hsm.NewStdEvent(EventPeerReplicateLog),
        message,
    }
}

type PeerActivateEvent struct {
    *hsm.StdEvent
}

func NewPeerActivateEvent() *PeerActivateEvent {
    return &PeerActivateEvent{
        hsm.NewStdEvent(EventPeerActivate),
    }
}

type PeerDeactivateEvent struct {
    *hsm.StdEvent
}

func NewPeerDeactivateEvent() *PeerDeactivateEvent {
    return &PeerDeactivateEvent{
        hsm.NewStdEvent(EventPeerDeactivate),
    }
}

type PeerEnterLeaderEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterLeaderEvent() *PeerEnterLeaderEvent {
    return &PeerEnterLeaderEvent{
        hsm.NewStdEvent(EventPeerEnterLeader),
    }
}

type PeerEnterSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterSnapshotModeEvent() *PeerEnterSnapshotModeEvent {
    return &PeerEnterSnapshotModeEvent{
        hsm.NewStdEvent(EventPeerEnterSnapshotMode),
    }
}

type PeerAbortSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerAbortSnapshotModeEvent() *PeerAbortSnapshotModeEvent {
    return &PeerAbortSnapshotModeEvent{
        hsm.NewStdEvent(EventPeerAbortSnapshotMode),
    }
}
