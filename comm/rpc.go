package comm

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "modules/rpcplus"
    "modules/rpcwrap"
    "modules/rpcwrap/auth"
    "modules/rpcwrap/msgpackrpc"
    "net"
    "net/http"
    "sync"
    "time"
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

type RPCRaftService struct {
    eventHandler RequestEventHandler
}

func NewRPCRaftService(eventHandler RequestEventHandler) *RPCRaftService {
    return &RPCRaftService{
        eventHandler: eventHandler,
    }
}

type RPCAppendEntriesRequest ev.AppendEntriesRequest
type RPCAppendEntriesResponse struct {
    Response *ev.AppendEntriesResponse
}
type RPCRequestVoteRequest ev.RequestVoteRequest
type RPCRequestVoteResponse struct {
    Response *ev.RequestVoteResponse
}
type RPCInstallSnapshotRequest ev.InstallSnapshotRequest
type RPCInstallSnapshotResponse struct {
    Response *ev.InstallSnapshotResponse
}

func (self *RPCRaftService) AppendEntries(
    args *RPCAppendEntriesRequest, reply *RPCAppendEntriesResponse) error {

    request := (*ev.AppendEntriesRequest)(args)
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.AppendEntriesResponseEvent)
    hsm.AssertTrue(ok)
    reply.Response = e.Response
    return nil
}

func (self *RPCRaftService) RequestVote(
    args *RPCRequestVoteRequest, reply *RPCRequestVoteResponse) error {

    request := (*ev.RequestVoteRequest)(args)
    reqEvent := ev.NewRequestVoteRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.RequestVoteResponseEvent)
    hsm.AssertTrue(ok)
    reply.Response = e.Response
    return nil
}

func (self *RPCRaftService) InstallStapshot(
    args *RPCInstallSnapshotRequest, reply *RPCInstallSnapshotResponse) error {

    request := (*ev.InstallSnapshotRequest)(args)
    reqEvent := ev.NewInstallSnapshotRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    e, ok := event.(*ev.InstallSnapshotResponseEvent)
    hsm.AssertTrue(ok)
    reply.Response = e.Response
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
    RPCResultLeaderInMemberChange
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
    case ev.EventLeaderInMemberChangeResponse:
        reply.Result = RPCResultLeaderInMemberChange
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

func getRPCClientResponse(reply *RPCClientResponse) (ev.Event, error) {
    var event ev.Event
    switch reply.Result {
    case RPCResultSuccess:
        response := &ev.ClientResponse{
            Success: true,
            Data:    reply.Data,
        }
        event = ev.NewClientResponseEvent(response)
    case RPCResultFail:
        response := &ev.ClientResponse{
            Success: false,
            Data:    reply.Data,
        }
        event = ev.NewClientResponseEvent(response)
    case RPCResultLeaderUnknown:
        event = ev.NewLeaderUnknownResponseEvent()
    case RPCResultLeaderUnsync:
        event = ev.NewLeaderUnsyncResponseEvent()
    case RPCResultLeaderRedirect:
        response := &ev.LeaderRedirectResponse{
            Leader: reply.Leader,
        }
        event = ev.NewLeaderRedirectResponseEvent(response)
    case RPCResultLeaderInMemberChange:
        event = ev.NewLeaderInMemberChangeResponseEvent()
    case RPCResultPersistError:
        err := errors.New(reply.Error)
        event = ev.NewPersistErrorResponseEvent(err)
    case RPCResultGetConfig:
        response := &ev.ClientGetConfigResponse{
            Conf: reply.Conf,
        }
        event = ev.NewClientGetConfigResponseEvent(response)
    default:
        return nil, RPCErrorInvalidResponse
    }
    return event, nil
}

type RPCClientService struct {
    eventHandler RequestEventHandler
}

func NewRPCClientService(eventHandler RequestEventHandler) *RPCClientService {
    return &RPCClientService{
        eventHandler: eventHandler,
    }
}

func (self *RPCClientService) Append(
    args *RPCClientAppendRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientAppendRequest)(args)
    reqEvent := ev.NewClientAppendRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClientService) ReadOnly(
    args *RPCClientReadOnlyRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientReadOnlyRequest)(args)
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClientService) GetConfig(
    args *RPCClientGetConfigRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientGetConfigRequest)(args)
    reqEvent := ev.NewClientGetConfigRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

func (self *RPCClientService) ChangeConfig(
    args *RPCClientChangeConfigRequest, reply *RPCClientResponse) error {

    request := (*ev.ClientChangeConfigRequest)(args)
    reqEvent := ev.NewClientChangeConfigRequestEvent(request)
    self.eventHandler(reqEvent)
    event := reqEvent.RecvResponse()
    setRPCClientResponse(event, reply)
    return nil
}

var (
    RPCErrorInvalidRequest        error = errors.New("invalid rpc request")
    RPCErrorInvalidResponse             = errors.New("invalid rpc response")
    RPCErrorNoConnectionForTarget       = errors.New("no connection for this target")
)

type RPCConnection struct {
    addr    net.Addr
    timeout time.Duration
    *rpcplus.Client
}

func NewRPCConnection(addr net.Addr, timeout time.Duration) *RPCConnection {
    object := &RPCConnection{
        addr:    addr,
        timeout: timeout,
    }
    return object
}

func (self *RPCConnection) Open() error {
    client, err := msgpackrpc.DialAuthHTTP(
        self.addr.Network(), self.addr.String(), "username", "pwd1", self.timeout, nil)
    if err != nil {
        return err
    }
    self.Client = client
    return nil
}

func (self *RPCConnection) Close() error {
    return self.Client.Close()
}

func (self *RPCConnection) PeerAddr() net.Addr {
    return self.addr
}

func (self *RPCConnection) CallRPC(
    request ev.Event) (response ev.Event, err error) {

    switch request.Type() {
    case ev.EventAppendEntriesRequest:
        e, ok := request.(*ev.AppendEntriesRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCAppendEntriesRequest)(e.Request)
        reply := new(RPCAppendEntriesResponse)
        err := self.Client.Call("RPCRaftService.AppendEntries", args, reply)
        if err != nil {
            return nil, err
        }
        event := ev.NewAppendEntriesResponseEvent(reply.Response)
        return event, nil
    case ev.EventRequestVoteRequest:
        e, ok := request.(*ev.RequestVoteRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCRequestVoteRequest)(e.Request)
        reply := new(RPCRequestVoteResponse)
        err := self.Client.Call("RPCRaftService.RequestVote", args, reply)
        if err != nil {
            return nil, err
        }
        event := ev.NewRequestVoteResponseEvent(reply.Response)
        return event, nil
    case ev.EventInstallSnapshotRequest:
        e, ok := request.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCInstallSnapshotRequest)(e.Request)
        reply := new(RPCInstallSnapshotResponse)
        err := self.Client.Call("RPCRaftService.InstallSnapshot", args, reply)
        if err != nil {
            return nil, err
        }
        event := ev.NewInstallSnapshotResponseEvent(reply.Response)
        return event, nil
    case ev.EventClientAppendRequest:
        e, ok := request.(*ev.ClientAppendRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCClientAppendRequest)(e.Request)
        reply := new(RPCClientResponse)
        err := self.Client.Call("RPCClientService.Append", args, reply)
        if err != nil {
            return nil, err
        }
        return getRPCClientResponse(reply)
    case ev.EventClientReadOnlyRequest:
        e, ok := request.(*ev.ClientReadOnlyRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCClientReadOnlyRequest)(e.Request)
        reply := new(RPCClientResponse)
        err := self.Client.Call("RPCClientService.ReadOnly", args, reply)
        if err != nil {
            return nil, err
        }
        return getRPCClientResponse(reply)
    case ev.EventClientGetConfigRequest:
        e, ok := request.(*ev.ClientGetConfigRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCClientGetConfigRequest)(e.Request)
        reply := new(RPCClientResponse)
        err := self.Client.Call("RPCClientService.GetConfig", args, reply)
        if err != nil {
            return nil, err
        }
        return getRPCClientResponse(reply)
    case ev.EventClientChangeConfigRequest:
        e, ok := request.(*ev.ClientChangeConfigRequestEvent)
        hsm.AssertTrue(ok)
        args := (*RPCClientChangeConfigRequest)(e.Request)
        reply := new(RPCClientResponse)
        err := self.Client.Call("RPCClientService.ChangeConfig", args, reply)
        if err != nil {
            return nil, err
        }
        return getRPCClientResponse(reply)
    default:
        return nil, RPCErrorInvalidRequest
    }
}

type RPCClient struct {
    timeout            time.Duration
    connectionPool     map[string]*RPCConnection
    connectionPoolLock sync.RWMutex
}

func NewRPCClient(timeout time.Duration) *RPCClient {
    return &RPCClient{
        timeout:        timeout,
        connectionPool: make(map[string]*RPCConnection),
    }
}

func (self *RPCClient) CallRPCTo(
    target net.Addr, request ev.Event) (response ev.Event, err error) {

    connection, err := self.getConnection(target)
    if err != nil {
        return nil, err
    }
    response, err = connection.CallRPC(request)
    if err == nil {
        self.returnConnectionToPool(connection)
    } else {
        connection.Close()
    }
    return response, err
}

func (self *RPCClient) getConnection(target net.Addr) (*RPCConnection, error) {
    connection, err := self.getConnectionFromPool(target)
    if err == nil {
        return connection, nil
    }

    connection = NewRPCConnection(target, self.timeout)
    if err := connection.Open(); err != nil {
        return nil, err
    }
    return connection, nil
}

func (self *RPCClient) getConnectionFromPool(
    target net.Addr) (*RPCConnection, error) {

    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()
    key := target.String()
    connection, ok := self.connectionPool[key]
    if ok {
        return connection, nil
    }
    return nil, RPCErrorNoConnectionForTarget
}

func (self *RPCClient) returnConnectionToPool(connection *RPCConnection) {
    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()
    key := connection.PeerAddr().String()
    self.connectionPool[key] = connection
}

func (self *RPCClient) Close() error {
    self.connectionPoolLock.Lock()
    defer self.connectionPoolLock.Unlock()
    var err error
    for targetString, connection := range self.connectionPool {
        err = connection.Close()
        delete(self.connectionPool, targetString)
    }
    return err
}

/* Copied from net.http.server.go */
// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type TcpKeepAliveListener struct {
    *net.TCPListener
}

func (ln TcpKeepAliveListener) Accept() (c net.Conn, err error) {
    tc, err := ln.AcceptTCP()
    if err != nil {
        return
    }
    tc.SetKeepAlive(true)
    tc.SetKeepAlivePeriod(3 * time.Minute)
    return tc, nil
}

type RPCServer struct {
    listener     net.Listener
    server       *http.Server
    group        sync.WaitGroup
    eventHandler RequestEventHandler
    logger       logging.Logger
}

func NewRPCServer(
    bindAddr net.Addr,
    timeout time.Duration,
    eventHandler RequestEventHandler,
    logger logging.Logger) (*RPCServer, error) {

    // Re-register the same service would return error.
    auth.LoadCredentialsFromJson([]byte(`{"username":["pwd1"]}`))
    rpcServer := rpcplus.NewServer()
    if err := rpcServer.Register(NewRPCRaftService(eventHandler)); err != nil {
        return nil, err
    }
    if err := rpcServer.Register(NewRPCClientService(eventHandler)); err != nil {
        return nil, err
    }
    serverMux := http.NewServeMux()
    rpcwrap.ServerServeRPC(
        serverMux, rpcServer, "msgpack", msgpackrpc.NewServerCodec)
    listener, err := net.Listen(bindAddr.Network(), bindAddr.String())
    if err != nil {
        return nil, err
    }
    server := &http.Server{
        Addr:         bindAddr.String(),
        Handler:      serverMux,
        ReadTimeout:  timeout,
        WriteTimeout: timeout,
    }
    object := &RPCServer{
        listener:     listener,
        server:       server,
        eventHandler: eventHandler,
        logger:       logger,
    }
    return object, nil
}

func (self *RPCServer) SetReadTimeout(timeout time.Duration) {
    self.server.ReadTimeout = timeout
}

func (self *RPCServer) SetWriteTimeout(timeout time.Duration) {
    self.server.WriteTimeout = timeout
}

func (self *RPCServer) Serve() {
    routine := func() {
        self.group.Add(1)
        defer self.group.Done()
        self.server.Serve(
            TcpKeepAliveListener{self.listener.(*net.TCPListener)})
    }
    go routine()
}

func (self *RPCServer) Close() error {
    err := self.listener.Close()
    self.group.Wait()
    return err
}
