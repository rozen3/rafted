package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

var (
    Success             error = nil
    Failure                   = errors.New("failure")
    Timeout                   = errors.New("timeout")
    LeaderUnsync              = errors.New("leader unsync")
    LeaderUnknown             = errors.New("leader Unknown")
    InMemberChange            = errors.New("in member change")
    PersistError              = errors.New("persist error")
    InvalidResponseType       = errors.New("invalid response type")
)

type Client interface {
    Append(data []byte) (result []byte, err error)
    ReadOnly(data []byte) (result []byte, err error)
    GetConfig() (servers []ps.ServerAddr, err error)
    ChangeConfig(oldServers []ps.ServerAddr, newServers []ps.ServerAddr) error
}

type SimpleClient struct {
    backend RaftBackend
    timeout time.Duration
}

func NewSimpleClient(backend RaftBackend, timeout time.Duration) *SimpleClient {
    return &SimpleClient{
        backend: backend,
        timeout: timeout,
    }
}

func (self *SimpleClient) Append(data []byte) (result []byte, err error) {
    request := &ev.ClientAppendRequest{
        Data: data,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    return doRequest(self.backend, reqEvent, self.timeout,
        dummyRedirectHandler, dummyInavaliableHandler)
}

func (self *SimpleClient) ReadOnly(data []byte) (result []byte, err error) {
    request := &ev.ClientReadOnlyRequest{
        Data: data,
    }
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    return doRequest(self.backend, reqEvent, self.timeout,
        dummyRedirectHandler, dummyInavaliableHandler)

}

func (self *SimpleClient) GetConfig() (servers []ps.ServerAddr, err error) {
    // TODO add impl
    return nil, nil
}

func (self *SimpleClient) ChangeConfig(
    oldServers []ps.ServerAddr, newServers []ps.ServerAddr) error {

    request := &ev.ClientMemberChangeRequest{
        OldServers: oldServers,
        NewServers: newServers,
    }
    reqEvent := ev.NewClientMemberChangeRequestEvent(request)
    _, err := doRequest(self.backend, reqEvent, self.timeout,
        dummyRedirectHandler, dummyInavaliableHandler)
    return err
}

type RedirectClient struct {
    timeout time.Duration
    delay   time.Duration

    backend RaftBackend
    client  cm.Client
    server  cm.Server
    logger  logging.Logger
}

func NewRedirectClient(
    timeout time.Duration,
    delay time.Duration,
    backend RaftBackend,
    client cm.Client,
    server cm.Server,
    logger logging.Logger) *RedirectClient {

    return &RedirectClient{
        timeout: timeout,
        delay:   delay,
        backend: backend,
        client:  client,
        server:  server,
        logger:  logger,
    }
}

func (self *RedirectClient) Start() error {
    go self.server.Serve()
    return nil
}

func (self *RedirectClient) Close() error {
    return self.server.Close()
}

func (self *RedirectClient) Append(data []byte) (result []byte, err error) {
    request := &ev.ClientAppendRequest{
        Data: data,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    redirectHandler := func(
        respEvent *ev.LeaderRedirectResponseEvent,
        reqEvent ev.RaftRequestEvent) (ev.RaftEvent, error) {

        return self.client.CallRPCTo(&respEvent.Response.Leader, reqEvent)
    }
    inavaliableHandler := func(
        respEvent ev.RaftEvent,
        reqEvent ev.RaftRequestEvent) ([]byte, error) {

        time.Sleep(self.delay)
        return nil, nil
    }
    return doRequest(self.backend, reqEvent, self.timeout,
        redirectHandler, inavaliableHandler)
}

func (self *RedirectClient) ReadOnly(data []byte) (result []byte, err error) {
    request := &ev.ClientReadOnlyRequest{
        Data: data,
    }
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    redirectHandler := func(
        respEvent *ev.LeaderRedirectResponseEvent,
        reqEvent ev.RaftRequestEvent) (ev.RaftEvent, error) {

        return self.client.CallRPCTo(&respEvent.Response.Leader, reqEvent)
    }
    inavaliableHandler := func(
        respEvent ev.RaftEvent,
        reqEvent ev.RaftRequestEvent) ([]byte, error) {

        time.Sleep(self.delay)
        return nil, nil
    }
    return doRequest(self.backend, reqEvent, self.timeout,
        redirectHandler, inavaliableHandler)
}

func (self *RedirectClient) GetConfig() (servers []ps.ServerAddr, err error) {
    // TODO add impl
    return nil, nil
}

func (self *RedirectClient) ChangeConfig(
    oldServers []ps.ServerAddr, newServers []ps.ServerAddr) error {

    request := &ev.ClientMemberChangeRequest{
        OldServers: oldServers,
        NewServers: newServers,
    }
    reqEvent := ev.NewClientMemberChangeRequestEvent(request)
    redirectHandler := func(
        respEvent *ev.LeaderRedirectResponseEvent,
        reqEvent ev.RaftRequestEvent) (ev.RaftEvent, error) {

        return self.client.CallRPCTo(&respEvent.Response.Leader, reqEvent)
    }
    inavaliableHandler := func(
        respEvent ev.RaftEvent,
        reqEvent ev.RaftRequestEvent) ([]byte, error) {

        time.Sleep(self.delay)
        return nil, nil
    }
    _, err := doRequest(self.backend, reqEvent, self.timeout,
        redirectHandler, inavaliableHandler)
    return err
}

func sendToBackend(
    backend RaftBackend,
    reqEvent ev.RaftRequestEvent,
    timeout time.Duration) (event ev.RaftEvent, err error) {

    backend.Send(reqEvent)
    timeChan := time.After(timeout)
    select {
    case event := <-reqEvent.GetResponseChan():
        return event, nil
    case <-timeChan:
        return nil, Timeout
    }
}

type RedirectResponseHandler func(
    *ev.LeaderRedirectResponseEvent, ev.RaftRequestEvent) (ev.RaftEvent, error)

type InavaliableResponseHandler func(
    ev.RaftEvent, ev.RaftRequestEvent) ([]byte, error)

func doRequest(
    backend RaftBackend,
    reqEvent ev.RaftRequestEvent,
    timeout time.Duration,
    redirectHandler RedirectResponseHandler,
    inavaliableHandler InavaliableResponseHandler) ([]byte, error) {

    for {
        respEvent, err := sendToBackend(backend, reqEvent, timeout)
        if err != nil {
            return nil, err
        }
        for {
            switch respEvent.Type() {
            case ev.EventClientResponse:
                e, ok := respEvent.(*ev.ClientResponseEvent)
                hsm.AssertTrue(ok)
                if e.Response.Success {
                    return e.Response.Data, nil
                }
                return nil, Failure
            case ev.EventLeaderRedirectResponse:
                e, ok := respEvent.(*ev.LeaderRedirectResponseEvent)
                hsm.AssertTrue(ok)
                respEvent, err = redirectHandler(e, reqEvent)
                if err != nil {
                    return nil, err
                }
            case ev.EventLeaderUnknownResponse:
                fallthrough
            case ev.EventLeaderUnsyncResponse:
                result, err := inavaliableHandler(respEvent, reqEvent)
                if err != nil {
                    return result, err
                }
                break
            case ev.EventLeaderInMemberChangeResponse:
                return nil, InMemberChange
            case ev.EventPersistErrorResponse:
                return nil, PersistError
            default:
                return nil, InvalidResponseType
            }
        }
    }
}

func dummyRedirectHandler(
    respEvent *ev.LeaderRedirectResponseEvent,
    _ ev.RaftRequestEvent) (ev.RaftEvent, error) {

    return respEvent, nil
}

func dummyInavaliableHandler(
    respEvent ev.RaftEvent, _ ev.RaftRequestEvent) ([]byte, error) {

    return nil, nil
}
