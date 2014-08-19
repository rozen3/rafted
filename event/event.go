package event

import (
    hsm "github.com/hhkbp2/go-hsm"
    ps "github.com/hhkbp2/rafted/persist"
)

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
    EventMemberChangeNextStep
    EventMemberChangeLogEntryCommit
    EventLeaderMemberChangeActivate
    EventLeaderMemberChangeDeactivate
    EventLeaderReenterMemberChangeState
    EventLeaderForwardMemberChangePhase
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
    EventLeaderInMemberChangeResponse
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
    case EventMemberChangeNextStep:
        return "MemberChangeNextStepEvent"
    case EventMemberChangeLogEntryCommit:
        return "MemberChangeLogEntryCommitEvent"
    case EventLeaderMemberChangeActivate:
        return "LeaderMemberChangeActivateEvent"
    case EventLeaderMemberChangeDeactivate:
        return "LeaderMemberChangeDeactivateEvent"
    case EventLeaderReenterMemberChangeState:
        return "LeaderReenterMemberChangeStateEvent"
    case EventLeaderForwardMemberChangePhase:
        return "LeaderForwardMemberChangePhaseEvent"
    case EventPeerReplicateLog:
        return "PeerReplicateLogEvent"
    case EventPeerActivate:
        return "ActivatePeerEvent"
    case EventPeerDeactivate:
        return "DeactivatePeerEvent"
    case EventPeerEnterLeader:
        return "PeerEnterLeaderEvent"
    case EventPeerEnterSnapshotMode:
        return "PeerEnterSanpshotModeEvent"
    case EventPeerAbortSnapshotMode:
        return "PeerAbortSnapshotModeEvent"
    case EventClientWriteRequest:
        return "ClientWriteRequestEvent"
    case EventClientReadOnlyRequest:
        return "ClientReadOnlyRequestEvent"
    case EventClientMemberChangeRequest:
        return "ClientMemberChangeRequestEvent"
    case EventClientResponse:
        return "ClientResponseEvent"
    case EventLeaderRedirectResponse:
        return "LeaderRedirectResponseEvent"
    case EventLeaderUnknownResponse:
        return "LeaderUnknownResponseEvent"
    case EventLeaderUnsyncResponse:
        return "LeaderUnsyncResponseEvent"
    case EventLeaderInMemberChangeResponse:
        return "LeaderInMemberChangeResponseEvent"
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
        StdEvent:   hsm.NewStdEvent(eventType),
        resultChan: make(chan RaftEvent, 1),
    }
}

type AppendEntriesReqeustEvent struct {
    *RaftRequestEventHead
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(
    request *AppendEntriesRequest) *AppendEntriesReqeustEvent {

    return &AppendEntriesReqeustEvent{
        RaftRequestEventHead: NewRaftRequestEventHead(
            EventAppendEntriesRequest),
        Request: request,
    }
}
func (self *AppendEntriesReqeustEvent) Message() interface{} {
    return self.Request
}

type AppendEntriesResponseEvent struct {
    *hsm.StdEvent
    FromAddr ps.ServerAddr
    Response *AppendEntriesResponse
}

func NewAppendEntriesResponseEvent(
    response *AppendEntriesResponse) *AppendEntriesResponseEvent {
    return &AppendEntriesResponseEvent{
        StdEvent: hsm.NewStdEvent(EventAppendEntriesResponse),
        Response: response,
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
        RaftRequestEventHead: NewRaftRequestEventHead(EventRequestVoteRequest),
        Request:              request,
    }
}

func (self *RequestVoteRequestEvent) Message() interface{} {
    return self.Request
}

type RequestVoteResponseEvent struct {
    *hsm.StdEvent
    FromAddr ps.ServerAddr
    Response *RequestVoteResponse
}

func NewRequestVoteResponseEvent(
    response *RequestVoteResponse) *RequestVoteResponseEvent {

    return &RequestVoteResponseEvent{
        StdEvent: hsm.NewStdEvent(EventRequestVoteResponse),
        Response: response,
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
        RaftRequestEventHead: NewRaftRequestEventHead(
            EventInstallSnapshotRequest),
        Request: request,
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
        StdEvent: hsm.NewStdEvent(EventInstallSnapshotResponse),
        Response: response,
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
        StdEvent: hsm.NewStdEvent(EventTimeoutHeartbeat),
        Message:  message,
    }
}

type ElectionTimeoutEvent struct {
    *hsm.StdEvent
}

func NewElectionTimeoutEvent() *ElectionTimeoutEvent {
    return &ElectionTimeoutEvent{
        hsm.NewStdEvent(EventTimeoutElection),
    }
}

type AbortSnapshotRecoveryEvent struct {
    *hsm.StdEvent
}

func NewAbortSnapshotRecoveryEvent() *AbortSnapshotRecoveryEvent {
    return &AbortSnapshotRecoveryEvent{
        StdEvent: hsm.NewStdEvent(EventAbortSnapshotRecovery),
    }
}

type StepdownEvent struct {
    *hsm.StdEvent
}

func NewStepdownEvent() *StepdownEvent {
    return &StepdownEvent{
        StdEvent: hsm.NewStdEvent(EventStepdown),
    }
}

type MemberChangeNextStepEvent struct {
    *hsm.StdEvent
    Message *MemberChangeNewConf
}

func NewMemberChangeNextStepEvent(
    message *MemberChangeNewConf) *MemberChangeNextStepEvent {

    return &MemberChangeNextStepEvent{
        StdEvent: hsm.NewStdEvent(EventMemberChangeNextStep),
        Message:  message,
    }
}

type MemberChangeLogEntryCommitEvent struct {
    *hsm.StdEvent
    Message *MemberChangeNewConf
}

func NewMemberChangeLogEntryCommitEvent(
    message *MemberChangeNewConf) *MemberChangeLogEntryCommitEvent {

    return &MemberChangeLogEntryCommitEvent{
        StdEvent: hsm.NewStdEvent(EventMemberChangeLogEntryCommit),
        Message:  message,
    }
}

type LeaderMemberChangeActivateEvent struct {
    *hsm.StdEvent
}

func NewLeaderMemberChangeActivateEvent() *LeaderMemberChangeActivateEvent {
    return &LeaderMemberChangeActivateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderMemberChangeActivate),
    }
}

type LeaderMemberChangeDeactivateEvent struct {
    *hsm.StdEvent
}

func NewLeaderMemberChangeDeactivateEvent() *LeaderMemberChangeDeactivateEvent {
    return &LeaderMemberChangeDeactivateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderMemberChangeDeactivate),
    }
}

type LeaderReenterMemberChangeStateEvent struct {
    *hsm.StdEvent
}

func NewLeaderReenterMemberChangeStateEvent() *LeaderReenterMemberChangeStateEvent {
    return &LeaderReenterMemberChangeStateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderReenterMemberChangeState),
    }
}

type LeaderForwardMemberChangePhaseEvent struct {
    *hsm.StdEvent
    Message *LeaderForwardMemberChangePhase
}

func NewLeaderForwardMemberChangePhaseEvent(
    message *LeaderForwardMemberChangePhase) *LeaderForwardMemberChangePhaseEvent {
    return &LeaderForwardMemberChangePhaseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderForwardMemberChangePhase),
        Message:  message,
    }
}

type PeerReplicateLogEvent struct {
    *hsm.StdEvent
    Message *PeerReplicateLog
}

func NewPeerReplicateLogEvent(
    message *PeerReplicateLog) *PeerReplicateLogEvent {

    return &PeerReplicateLogEvent{
        StdEvent: hsm.NewStdEvent(EventPeerReplicateLog),
        Message:  message,
    }
}

type PeerActivateEvent struct {
    *hsm.StdEvent
}

func NewPeerActivateEvent() *PeerActivateEvent {
    return &PeerActivateEvent{
        StdEvent: hsm.NewStdEvent(EventPeerActivate),
    }
}

type PeerDeactivateEvent struct {
    *hsm.StdEvent
}

func NewPeerDeactivateEvent() *PeerDeactivateEvent {
    return &PeerDeactivateEvent{
        StdEvent: hsm.NewStdEvent(EventPeerDeactivate),
    }
}

type PeerEnterLeaderEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterLeaderEvent() *PeerEnterLeaderEvent {
    return &PeerEnterLeaderEvent{
        StdEvent: hsm.NewStdEvent(EventPeerEnterLeader),
    }
}

type PeerEnterSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterSnapshotModeEvent() *PeerEnterSnapshotModeEvent {
    return &PeerEnterSnapshotModeEvent{
        StdEvent: hsm.NewStdEvent(EventPeerEnterSnapshotMode),
    }
}

type PeerAbortSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerAbortSnapshotModeEvent() *PeerAbortSnapshotModeEvent {
    return &PeerAbortSnapshotModeEvent{
        StdEvent: hsm.NewStdEvent(EventPeerAbortSnapshotMode),
    }
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
        StdEvent:   hsm.NewStdEvent(eventType),
        ResultChan: make(chan ClientEvent, 1),
    }
}

type ClientWriteRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientWriteRequest
}

func NewClientWriteRequestEvent(
    request *ClientWriteRequest) *ClientWriteRequestEvent {

    return &ClientWriteRequestEvent{
        ClientRequestEventHead: NewClientRequestEventHead(
            EventClientWriteRequest),
        Request: request,
    }
}

type ClientReadOnlyRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientReadOnlyRequest
}

func NewClientReadOnlyRequestEvent(
    request *ClientReadOnlyRequest) *ClientReadOnlyRequestEvent {

    return &ClientReadOnlyRequestEvent{
        ClientRequestEventHead: NewClientRequestEventHead(
            EventClientReadOnlyRequest),
        Request: request,
    }
}

type ClientMemberChangeRequestEvent struct {
    *ClientRequestEventHead
    Request *ClientMemberChangeRequest
}

func NewClientMemberChangeRequestEvent(
    request *ClientMemberChangeRequest) *ClientMemberChangeRequestEvent {

    return &ClientMemberChangeRequestEvent{
        ClientRequestEventHead: NewClientRequestEventHead(
            EventClientMemberChangeRequest),
        Request: request,
    }
}

type ClientResponseEvent struct {
    *hsm.StdEvent
    Response *ClientResponse
}

func NewClientResponseEvent(
    response *ClientResponse) *ClientResponseEvent {

    return &ClientResponseEvent{
        StdEvent: hsm.NewStdEvent(EventClientResponse),
        Response: response,
    }
}

type LeaderRedirectResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderRedirectResponse
}

func NewLeaderRedirectResponseEvent(
    response *LeaderRedirectResponse) *LeaderRedirectResponseEvent {
    return &LeaderRedirectResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderRedirectResponse),
        Response: response,
    }
}

type LeaderUnknownResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderUnknownResponse
}

func NewLeaderUnknownResponseEvent(
    response *LeaderUnknownResponse) *LeaderUnknownResponseEvent {

    return &LeaderUnknownResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderUnknownResponse),
        Response: response,
    }
}

type LeaderUnsyncResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderUnsyncResponse
}

func NewLeaderUnsyncResponseEvent(
    response *LeaderUnsyncResponse) *LeaderUnsyncResponseEvent {

    return &LeaderUnsyncResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderUnsyncResponse),
        Response: response,
    }
}

type LeaderInMemberChangeResponseEvent struct {
    *hsm.StdEvent
    Response *LeaderInMemberChangeResponse
}

func NewLeaderInMemberChangeResponseEvent(
    response *LeaderInMemberChangeResponse) *LeaderInMemberChangeResponseEvent {

    return &LeaderInMemberChangeResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderInMemberChangeResponse),
        Response: response,
    }
}
