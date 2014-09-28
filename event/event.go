package event

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ps "github.com/zonas/rafted/persist"
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
    EventQueryStateRequest
    EventQueryStateResponse
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
    EventPersistError
    EventInternalEnd
    EventClientRequestBegin
    EventClientAppendRequest
    EventClientReadOnlyRequest
    EventClientGetConfigRequest
    EventClientChangeConfigRequest
    EventClientRequestEnd
    EventClientResponse
    EventClientGetConfigResponse
    EventLeaderRedirectResponse
    EventLeaderUnknownResponse
    EventLeaderUnsyncResponse
    EventLeaderInMemberChangeResponse
    EventPersistErrorResponse
    EventClientUser = hsm.EventUser + 1000 + iota
)

func EventString(event hsm.Event) string {
    return EventTypeString(event.Type())
}

func EventTypeString(event hsm.EventType) string {
    switch event {
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
    case EventQueryStateRequest:
        return "QueryStateRequestEvent"
    case EventQueryStateResponse:
        return "QueryStateResponseEvent"
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
    case EventPersistError:
        return "PersistErrorEvent"
    case EventClientAppendRequest:
        return "ClientAppendRequestEvent"
    case EventClientReadOnlyRequest:
        return "ClientReadOnlyRequestEvent"
    case EventClientGetConfigRequest:
        return "ClientGetConfigRequestEvent"
    case EventClientChangeConfigRequest:
        return "ClientChangeConfigRequestEvent"
    case EventClientResponse:
        return "ClientResponseEvent"
    case EventClientGetConfigResponse:
        return "ClientGetConfigResponseEvent"
    case EventLeaderRedirectResponse:
        return "LeaderRedirectResponseEvent"
    case EventLeaderUnknownResponse:
        return "LeaderUnknownResponseEvent"
    case EventLeaderUnsyncResponse:
        return "LeaderUnsyncResponseEvent"
    case EventLeaderInMemberChangeResponse:
        return "LeaderInMemberChangeResponseEvent"
    case EventPersistErrorResponse:
        return "PersistErrorResponseEvent"
    default:
        return fmt.Sprintf("Unknown Event: %d", event)
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

// ------------------------------------------------------------
// Raft Events
// ------------------------------------------------------------

type Event interface {
    hsm.Event
    Message() interface{}
}

type RequestEvent interface {
    Event
    SendResponse(Event)
    RecvResponse() Event
    GetResponseChan() <-chan Event
}

type RequestEventHead struct {
    *hsm.StdEvent
    ResultChan chan Event
}

func (self *RequestEventHead) SendResponse(event Event) {
    self.ResultChan <- event
}

func (self *RequestEventHead) RecvResponse() Event {
    response := <-self.ResultChan
    return response
}

func (self *RequestEventHead) GetResponseChan() <-chan Event {
    return self.ResultChan
}

func NewRequestEventHead(eventType hsm.EventType) *RequestEventHead {
    return &RequestEventHead{
        StdEvent:   hsm.NewStdEvent(eventType),
        ResultChan: make(chan Event, 1),
    }
}

// Event for AppendEntriesRequest message.
type AppendEntriesRequestEvent struct {
    *RequestEventHead
    Request *AppendEntriesRequest
}

func NewAppendEntriesRequestEvent(
    request *AppendEntriesRequest) *AppendEntriesRequestEvent {

    return &AppendEntriesRequestEvent{
        RequestEventHead: NewRequestEventHead(EventAppendEntriesRequest),
        Request:          request,
    }
}
func (self *AppendEntriesRequestEvent) Message() interface{} {
    return self.Request
}

// Event for AppendEntriesResponse message.
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

// Event for RequestVoteRequest message.
type RequestVoteRequestEvent struct {
    *RequestEventHead
    Request *RequestVoteRequest
}

func NewRequestVoteRequestEvent(
    request *RequestVoteRequest) *RequestVoteRequestEvent {

    return &RequestVoteRequestEvent{
        RequestEventHead: NewRequestEventHead(EventRequestVoteRequest),
        Request:          request,
    }
}

func (self *RequestVoteRequestEvent) Message() interface{} {
    return self.Request
}

// Event for RequestVoteResponse message.
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

// Event for InstallSnapshotRequest message.
type InstallSnapshotRequestEvent struct {
    *RequestEventHead
    Request *InstallSnapshotRequest
}

func NewInstallSnapshotRequestEvent(
    request *InstallSnapshotRequest) *InstallSnapshotRequestEvent {
    return &InstallSnapshotRequestEvent{
        RequestEventHead: NewRequestEventHead(EventInstallSnapshotRequest),
        Request:          request,
    }
}

func (self *InstallSnapshotRequestEvent) Message() interface{} {
    return self.Request
}

// Event for InstallSnapshotResponse message.
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

// ------------------------------------------------------------
// Client Events
// ------------------------------------------------------------
// Event for ClientAppendRequest message.
type ClientAppendRequestEvent struct {
    *RequestEventHead
    Request *ClientAppendRequest
}

func NewClientAppendRequestEvent(
    request *ClientAppendRequest) *ClientAppendRequestEvent {

    return &ClientAppendRequestEvent{
        RequestEventHead: NewRequestEventHead(EventClientAppendRequest),
        Request:          request,
    }
}

func (self *ClientAppendRequestEvent) Message() interface{} {
    return self.Request
}

// Event for ClientReadOnlyRequest message.
type ClientReadOnlyRequestEvent struct {
    *RequestEventHead
    Request *ClientReadOnlyRequest
}

func NewClientReadOnlyRequestEvent(
    request *ClientReadOnlyRequest) *ClientReadOnlyRequestEvent {

    return &ClientReadOnlyRequestEvent{
        RequestEventHead: NewRequestEventHead(EventClientReadOnlyRequest),
        Request:          request,
    }
}

func (self *ClientReadOnlyRequestEvent) Message() interface{} {
    return self.Request
}

type ClientGetConfigRequestEvent struct {
    *RequestEventHead
    Request *ClientGetConfigRequest
}

func NewClientGetConfigRequestEvent(
    request *ClientGetConfigRequest) *ClientGetConfigRequestEvent {

    return &ClientGetConfigRequestEvent{
        RequestEventHead: NewRequestEventHead(EventClientGetConfigRequest),
        Request:          request,
    }
}

func (self *ClientGetConfigRequestEvent) Message() interface{} {
    return nil
}

// Event for ClientChangeConfigRequest message.
type ClientChangeConfigRequestEvent struct {
    *RequestEventHead
    Request *ClientChangeConfigRequest
}

func NewClientChangeConfigRequestEvent(
    request *ClientChangeConfigRequest) *ClientChangeConfigRequestEvent {

    return &ClientChangeConfigRequestEvent{
        RequestEventHead: NewRequestEventHead(EventClientChangeConfigRequest),
        Request:          request,
    }
}

func (self *ClientChangeConfigRequestEvent) Message() interface{} {
    return self.Request
}

// ClientResponseEvent is the general response event to client.
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

func (self *ClientResponseEvent) Message() interface{} {
    return self.Response
}

type ClientGetConfigResponseEvent struct {
    *hsm.StdEvent
    Response *ClientGetConfigResponse
}

func NewClientGetConfigResponseEvent(
    response *ClientGetConfigResponse) *ClientGetConfigResponseEvent {

    return &ClientGetConfigResponseEvent{
        StdEvent: hsm.NewStdEvent(EventClientGetConfigResponse),
        Response: response,
    }
}

func (self *ClientGetConfigResponseEvent) Message() interface{} {
    return self.Response
}

// LeaderRedirectResponseEvent is to tell client we are not leader and
// the leader address at this moment for client to redirect.
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

func (self *LeaderRedirectResponseEvent) Message() interface{} {
    return self.Response
}

// LeaderUnknownResponseEvent is to tell client we are not leader and
// don't known which node in cluster is leader at this moment.
type LeaderUnknownResponseEvent struct {
    *hsm.StdEvent
}

func NewLeaderUnknownResponseEvent() *LeaderUnknownResponseEvent {
    return &LeaderUnknownResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderUnknownResponse),
    }
}

func (self *LeaderUnknownResponseEvent) Message() interface{} {
    return nil
}

// LeaderUnsyncResponseEvent is to tell client we are leader but not
// synchronized with other nodes in cluster yet, so we cannot handle
// read-only request at this moment.
// To handle this situation, it's a good for client to retry the read-only request.
type LeaderUnsyncResponseEvent struct {
    *hsm.StdEvent
}

func NewLeaderUnsyncResponseEvent() *LeaderUnsyncResponseEvent {
    return &LeaderUnsyncResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderUnsyncResponse),
    }
}

func (self *LeaderUnsyncResponseEvent) Message() interface{} {
    return nil
}

// LeaderInMemberChangeResponseEvent is to tell client the leader is already
// in a member change procedure and doesn't accept another member change
// request before finish the previous.
type LeaderInMemberChangeResponseEvent struct {
    *hsm.StdEvent
}

func NewLeaderInMemberChangeResponseEvent() *LeaderInMemberChangeResponseEvent {
    return &LeaderInMemberChangeResponseEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderInMemberChangeResponse),
    }
}

func (self *LeaderInMemberChangeResponseEvent) Message() interface{} {
    return nil
}

// PersistErrorResponseEvent is to tell client that a persist error happened.
// It's probably a hard disk failure. This module should be restarted and
// the whole hsm should be re-instablished after fixing the persist error.
type PersistErrorResponseEvent struct {
    *hsm.StdEvent
    Error error
}

func NewPersistErrorResponseEvent(err error) *PersistErrorResponseEvent {
    return &PersistErrorResponseEvent{
        StdEvent: hsm.NewStdEvent(EventPersistErrorResponse),
        Error:    err,
    }
}

func (self *PersistErrorResponseEvent) Message() interface{} {
    return self.Error
}

// ------------------------------------------------------------
// Internal Events
// ------------------------------------------------------------

type QueryStateRequestEvent struct {
    *RequestEventHead
}

func NewQueryStateRequestEvent() *QueryStateRequestEvent {
    return &QueryStateRequestEvent{
        RequestEventHead: NewRequestEventHead(EventQueryStateRequest),
    }
}

func (self *QueryStateRequestEvent) Message() interface{} {
    return nil
}

type QueryStateResponseEvent struct {
    *hsm.StdEvent
    Response *QueryStateResponse
}

func NewQueryStateResponseEvent(
    response *QueryStateResponse) *QueryStateResponseEvent {

    return &QueryStateResponseEvent{
        StdEvent: hsm.NewStdEvent(EventQueryStateResponse),
        Response: response,
    }
}

func (self *QueryStateResponseEvent) Message() interface{} {
    return self.Response
}

// HeartbeatTimeoutEvent is the event for heartbeat timeout.
type HeartbeatTimeoutEvent struct {
    *hsm.StdEvent
    Message *Timeout
}

func NewHeartbeatTimeoutEvent(message *Timeout) *HeartbeatTimeoutEvent {
    return &HeartbeatTimeoutEvent{
        StdEvent: hsm.NewStdEvent(EventTimeoutHeartbeat),
        Message:  message,
    }
}

// ElectionTimeoutEvent is the event for election timeout.
type ElectionTimeoutEvent struct {
    *hsm.StdEvent
    Message *Timeout
}

func NewElectionTimeoutEvent(message *Timeout) *ElectionTimeoutEvent {
    return &ElectionTimeoutEvent{
        StdEvent: hsm.NewStdEvent(EventTimeoutElection),
        Message:  message,
    }
}

// AbortSnapshotRecoveryEvent is an event for snapshot recovery state to exit.
type AbortSnapshotRecoveryEvent struct {
    *hsm.StdEvent
    Error error
}

func NewAbortSnapshotRecoveryEvent(err error) *AbortSnapshotRecoveryEvent {
    return &AbortSnapshotRecoveryEvent{
        StdEvent: hsm.NewStdEvent(EventAbortSnapshotRecovery),
        Error:    err,
    }
}

// StepdownEvent is an event for candidate state or leader state to
// transfer back to follower state.
type StepdownEvent struct {
    *hsm.StdEvent
}

func NewStepdownEvent() *StepdownEvent {
    return &StepdownEvent{
        StdEvent: hsm.NewStdEvent(EventStepdown),
    }
}

// MemberChangeNextStepEvent is an event to signal follower state to
// move forward during member change procedure.
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

// MemberChangeLogEntryCommitEvent is an event to signal follower state
// during member change procedure that the transitional configuration
// C[old,new] or C[new] is already committed.
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

// LeaderMemberChangeActivateEvent is an event to activate
// the member change orthogonal component of leader state.
type LeaderMemberChangeActivateEvent struct {
    *hsm.StdEvent
}

func NewLeaderMemberChangeActivateEvent() *LeaderMemberChangeActivateEvent {
    return &LeaderMemberChangeActivateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderMemberChangeActivate),
    }
}

// LeaderMemberChangeDeactivateEvent is an event to deactivate
// the member change orthogonal component of leader state.
type LeaderMemberChangeDeactivateEvent struct {
    *hsm.StdEvent
}

func NewLeaderMemberChangeDeactivateEvent() *LeaderMemberChangeDeactivateEvent {
    return &LeaderMemberChangeDeactivateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderMemberChangeDeactivate),
    }
}

// LeaderReenterMemberChangeStateEvent is an event to signal leader state
// it's a re-entry to member change procedure.
type LeaderReenterMemberChangeStateEvent struct {
    *hsm.StdEvent
}

func NewLeaderReenterMemberChangeStateEvent() *LeaderReenterMemberChangeStateEvent {
    return &LeaderReenterMemberChangeStateEvent{
        StdEvent: hsm.NewStdEvent(EventLeaderReenterMemberChangeState),
    }
}

// LeaderForwardMemberChangePhaseEvent is an event to signal leader state to
// move forward during member change procedure.
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

// PeerReplicateLogEvent is an event for a peer to signal leader that
// it make progress on replicating logs.
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

// PeerActivateEvent is an event to activate peers.
type PeerActivateEvent struct {
    *hsm.StdEvent
}

func NewPeerActivateEvent() *PeerActivateEvent {
    return &PeerActivateEvent{
        StdEvent: hsm.NewStdEvent(EventPeerActivate),
    }
}

// PeerDeactivateEvent is an event to deactivate peers.
type PeerDeactivateEvent struct {
    *hsm.StdEvent
}

func NewPeerDeactivateEvent() *PeerDeactivateEvent {
    return &PeerDeactivateEvent{
        StdEvent: hsm.NewStdEvent(EventPeerDeactivate),
    }
}

// PeerEnterLeaderEvent is an event to signal peer to enter leader mode.
type PeerEnterLeaderEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterLeaderEvent() *PeerEnterLeaderEvent {
    return &PeerEnterLeaderEvent{
        StdEvent: hsm.NewStdEvent(EventPeerEnterLeader),
    }
}

// PeerEnterSnapshotModeEvent is an event to signal peer to
// enter snapshot mode.
type PeerEnterSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerEnterSnapshotModeEvent() *PeerEnterSnapshotModeEvent {
    return &PeerEnterSnapshotModeEvent{
        StdEvent: hsm.NewStdEvent(EventPeerEnterSnapshotMode),
    }
}

// PeerAbortSnapshotModeEvent is an event to signal peer to exit
// snapshot mode.
type PeerAbortSnapshotModeEvent struct {
    *hsm.StdEvent
}

func NewPeerAbortSnapshotModeEvent() *PeerAbortSnapshotModeEvent {
    return &PeerAbortSnapshotModeEvent{
        StdEvent: hsm.NewStdEvent(EventPeerAbortSnapshotMode),
    }
}

// PersistErrorEvent is an event to signal persist error.
// Probably a hard disk failure.
type PersistErrorEvent struct {
    *hsm.StdEvent
    Error error
}

func NewPersistErrorEvent(err error) *PersistErrorEvent {
    return &PersistErrorEvent{
        StdEvent: hsm.NewStdEvent(EventPersistError),
        Error:    err,
    }
}
