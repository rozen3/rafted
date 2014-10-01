package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    cm "github.com/hhkbp2/rafted/comm"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/mock"
    "testing"
)

type MockLocal struct {
    mock.Mock

    log           ps.Log
    stateMachine  ps.StateMachine
    configManager ps.ConfigManager
    notifier      *Notifier
    Events        []hsm.Event
    PriorEvents   []hsm.Event
}

func NewMockLocal(
    log ps.Log,
    stateMachine ps.StateMachine,
    configManager ps.ConfigManager,
    notifier *Notifier) *MockLocal {

    return &MockLocal{
        log:           log,
        configManager: configManager,
        notifier:      notifier,
        Events:        make([]hsm.Event, 0),
        PriorEvents:   make([]hsm.Event, 0),
    }
}

func (self *MockLocal) Send(event hsm.Event) {
    self.Events = append(self.Events, event)
    self.Mock.Called(event)
}

func (self *MockLocal) SendPrior(event hsm.Event) {
    self.PriorEvents = append(self.PriorEvents, event)
    self.Mock.Called(event)
}

func (self *MockLocal) Close() error {
    args := self.Mock.Called()
    return args.Error(0)
}

func (self *MockLocal) QueryState() string {
    args := self.Mock.Called()
    return args.String(0)
}

func (self *MockLocal) GetCurrentTerm() uint64 {
    args := self.Mock.Called()
    return args.Uint64(0)
}

func (self *MockLocal) GetLocalAddr() *ps.ServerAddress {
    args := self.Mock.Called()
    var s *ps.ServerAddress
    var ok bool
    if s, ok = args.Get(0).(*ps.ServerAddress); !ok {
        panic(fmt.Sprintf("argument: %s not correct type MultiAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) GetVotedFor() *ps.ServerAddress {
    args := self.Mock.Called()
    var s *ps.ServerAddress
    var ok bool
    if s, ok = args.Get(0).(*ps.ServerAddress); !ok {
        panic(fmt.Sprintf("argument: %s not correct type MultiAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) GetLeader() *ps.ServerAddress {
    args := self.Mock.Called()
    var s *ps.ServerAddress
    var ok bool
    if s, ok = args.Get(0).(*ps.ServerAddress); !ok {
        panic(fmt.Sprintf("argument: %s not correct type MultiAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) Log() ps.Log {
    return self.log
}

func (self *MockLocal) StateMachine() ps.StateMachine {
    return self.stateMachine
}

func (self *MockLocal) ConfigManager() ps.ConfigManager {
    return self.configManager
}

func (self *MockLocal) Notifier() *Notifier {
    return self.notifier
}

func (self *MockLocal) SetPeers(peers Peers) {
    self.Mock.Called(peers)
}

func getTestMemoryServer(
    addr *ps.ServerAddress,
    eventHandler cm.RequestEventHandler) *cm.MemoryServer {

    logger := logging.GetLogger("test server #" + addr.String())
    server := cm.NewMemoryServer(
        addr,
        testConfig.CommServerTimeout,
        eventHandler,
        testRegister,
        logger)
    server.Serve()
    return server
}

func getTestPeerAndLocal() (Peer, *MockLocal, error) {
    servers := testServers
    index := testIndex
    term := testTerm
    conf := &ps.Config{
        Servers:    servers,
        NewServers: nil,
    }
    entry := &ps.LogEntry{
        Term:  term,
        Index: index,
        Type:  ps.LogCommand,
        Data:  testData,
        Conf:  conf,
    }
    log, err := getTestLog(index, index, entry)
    if err != nil {
        return nil, nil, err
    }
    stateMachine := ps.NewMemoryStateMachine()
    configManager := ps.NewMemoryConfigManager(index, conf)
    notifier := NewNotifier()
    local := NewMockLocal(log, stateMachine, configManager, notifier)
    client := cm.NewMemoryClient(
        testConfig.CommPoolSize, testConfig.CommClientTimeout, testRegister)
    logger := logging.GetLogger("test peer")

    peer := NewPeerMan(
        testConfig,
        servers.Addresses[1],
        client,
        local,
        logger)
    return peer, local, nil
}

func getTestPeerAndLocalSafe(t *testing.T) (Peer, *MockLocal) {
    peer, local, err := getTestPeerAndLocal()
    assert.Nil(t, err)
    return peer, local
}
