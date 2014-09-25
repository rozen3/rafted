package comm

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    // "modules/rpcplus"
    // "modules/rpcwrap"
    // "modules/rpcwrap/auth"
    // "net/http"
)

/**
------------------------------------------------------------
Request                         Response
------------------------------------------------------------
ClientAppend                    ClientResponse, with Data
                                LeaderUnknown
                                LeaderRedirect
                                PersistError

ClientReadOnly                  ClientResponse, with Data
                                LeaderUnknown
                                LeaderUnsync
                                LeaderRedirect
                                PersistError

ClientGetConfig                 GetConfigResponse, with Conf
                                LeaderUnknown
                                LeaderRedirect
                                PersistError

ClientChangeConfig              ClientResponse, no Data
                                Redirect
                                PersistError

------------------------------------------------------------
map to RPC
------------------------------------------------------------
RPCClientAppend                 RPCClientResponse
RPCClientReadOnly
RPCClientGetConfig
RPCClientChangeConfig
*/

type RPCRaft struct {
    eventHandler RequestEventHandler
}

func NewRPCRaft(eventHandler RequestEventHandler) *RPCRaft {
    return &RPCRaft{
        eventHandler: eventHandler,
    }
}

type RPCAppendEntriesRequest ev.AppendEntriesRequest
type RPCAppendEntriesResponse ev.AppendEntriesResponse
type RPCRequestVoteRequest ev.RequestVoteRequest
type RPCRequestVoteResponse ev.RequestVoteResponse
type RPCInstallSnapshotRequest ev.InstallSnapshotRequest
type RPCInstallSnapshotResponse ev.InstallSnapshotResponse

func (self *RPCRaft) AppendEntires(
    args *RPCAppendEntriesRequest, reply *RPCAppendEntriesResponse) error {

    request := (*ev.AppendEntriesRequest)(args)
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    hsm.AssertTrue(ok)
    reply = (*RPCAppendEntriesResponse)(e.Response)
    return nil
}

func (self *RPCRaft) RequestVote(
    args *RPCRequestVoteRequest, reply *RPCRequestVoteResponse) error {

    request := (*ev.RequestVoteRequest)(args)
    reqEvent := ev.NewRequestVoteRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.RequestVoteResponseEvent)
    hsm.AssertTrue(ok)
    reply = (*RPCRequestVoteResponse)(e.Response)
    return nil
}

func (self *RPCRaft) InstallStapshot(
    args *RPCInstallSnapshotRequest, reply *RPCInstallSnapshotResponse) error {

    request := (*ev.InstallSnapshotRequest)(args)
    reqEvent := ev.NewInstallSnapshotRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.InstallSnapshotResponseEvent)
    hsm.AssertTrue(ok)
    reply = (*RPCInstallSnapshotResponse)(e.Response)
    return nil
}

type RPCClientAppendRequest ev.ClientAppendRequest
type RPCClientReadOnlyRequest ev.ClientReadOnlyRequest
type RPCClientGetConfigRequest ev.ClientGetConfigRequest
type RPCClientChangeConfigRequest ev.ClientChangeConfigRequest

type RPCResultType int

const (
    RPCResultUnknown RPCResultType = iota
    RPCResultSuccess
    RPCResultFail
    RPCResultLeaderUnknown
    RPCResultLeaderUnsync
    RPCResultLeaderRedirect
    RPCResultInMemberChange
    RPCResultPersistError
    RPCResultGetConfig
)

type RPCClientResponse struct {
    Result RPCResultType
    Data   []byte
    Conf   *ps.Config
    Leader ps.ServerAddr
    Error  string
}

func setRPCClientResponse(event ev.Event, reply *RPCClientResponse) {
    switch event.Type() {
    case ev.EventClientResponse:
        e, ok := event.(*ev.ClientResponseEvent)
        hsm.AssertTrue(ok)
        if e.Response.Success {
            reply.Result = RPCResultSuccess
        } else {
            reply.Result = RPCResultFail
        }
        reply.Data = e.Response.Data
    case ev.EventLeaderUnknownResponse:
        reply.Result = RPCResultLeaderUnknown
    case ev.EventLeaderUnsyncResponse:
        reply.Result = RPCResultLeaderUnsync
    case ev.EventLeaderRedirectResponse:
        e, ok := event.(*ev.LeaderRedirectResponseEvent)
        hsm.AssertTrue(ok)
        reply.Result = RPCResultLeaderRedirect
        reply.Leader = e.Response.Leader
    case ev.EventPersistErrorResponse:
        e, ok := event.(*ev.PersistErrorResponseEvent)
        hsm.AssertTrue(ok)
        reply.Result = RPCResultPersistError
        reply.Error = e.Error.Error()
    case ev.EventClientGetConfigResponse:
        e, ok := event.(*ev.ClientGetConfigResponseEvent)
        hsm.AssertTrue(ok)
        reply.Result = RPCResultGetConfig
        reply.Conf = e.Response.Conf
    default:
        reply.Result = RPCResultUnknown
    }
}

type RPCClient struct {
    eventHandler RequestEventHandler
}

func NewRPCClient(eventHandler RequestEventHandler) *RPCClient {
    return &RPCClient{
        eventHandler: eventHandler,
    }
}

func (self *RPCClient) Append(
    args *RPCClientAppendRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientAppendRequest)(args)
    reqEvent := ev.NewClientAppendRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClient) ReadOnly(
    args *RPCClientReadOnlyRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientReadOnlyRequest)(args)
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClient) GetConfig(
    args *RPCClientGetConfigRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientGetConfigRequest)(args)
    reqEvent := ev.NewClientGetConfigRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClient) ChangeConfig(
    args *RPCClientChangeConfigRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientChangeConfigRequest)(args)
    reqEvent := ev.NewClientChangeConfigRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}
