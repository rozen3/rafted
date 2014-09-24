package comm

import (
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

type RPCRaft int

type RPCAppendEntriesRequest ev.AppendEntriesRequest
type RPCAppendEntriesResponse ev.AppendEntriesResponse
type RPCRequestVoteRequest ev.RequestVoteRequest
type RPCRequestVoteResponse ev.RequestVoteResponse
type RPCInstallSnapshotRequest ev.InstallSnapshotRequest
type RPCInstallSnapshotResponse ev.InstallSnapshotResponse

func (self *RPCRaft) AppendEntires(
    args *RPCAppendEntriesRequest, reply *RPCAppendEntriesResponse) error {
    // TODO add impl
    return nil
}

func (self *RPCRaft) RequestVote(
    args *RPCRequestVoteRequest, reply *RPCRequestVoteResponse) error {
    // TODO add impl
    return nil
}

func (self *RPCRaft) InstallStapshot(
    args *RPCInstallSnapshotRequest, reply *RPCInstallSnapshotResponse) error {
    // TODO add impl
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
    RPCResultLeaderUnknown
    RPCResultLeaderUnsync
    RPCResultLeaderRedirect
    RPCResultInMemberChange
    RPCResultPersistError
)

type RPCClientResponse struct {
    Result RPCResultType
    Data   []byte
    Conf   *ps.Config
    Leader ps.ServerAddr
    Error  string
}

type RPCClient struct {
    eventHandler RaftRequestEventHandler
}

func NewRPCClient(eventHandler RaftRequestEventHandler) *RPCClient {
    return &RPCClient{
        eventHandler: eventHandler,
    }
}

func (self *RPCClient) Append(
    args *RPCClientAppendRequest, reply *RPCClientResponse) error {

    // TODO add impl
    return nil
}

func (self *RPCClient) ReadOnly(
    args *RPCClientReadOnlyRequest, reply *RPCClientResponse) error {

    // TODO add impl
    return nil
}

func (self *RPCClient) GetConfig(
    args *RPCClientReadOnlyRequest, reply *RPCClientResponse) error {

    // TODO add impl
    return nil
}

func (self *RPCClient) ChangeConfig(
    args *RPCClientChangeConfigRequest, reply *RPCClientResponse) error {

    // TODO add impl
    return nil
}
