package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    "github.com/hhkbp2/rafted/comm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/mock"
    "testing"
)

type MockLocal struct {
    mock.Mock

    log             ps.Log
    snapshotManager ps.SnapshotManager
    configManager   ps.ConfigManager
    notifier        *Notifier
    Events          []hsm.Event
    PriorEvents     []hsm.Event
}

func NewMockLocal(
    log ps.Log,
    snapshotManager ps.SnapshotManager,
    configManager ps.ConfigManager,
    notifier *Notifier) *MockLocal {

    return &MockLocal{
        log:             log,
        snapshotManager: snapshotManager,
        configManager:   configManager,
        notifier:        notifier,
        Events:          make([]hsm.Event, 0),
        PriorEvents:     make([]hsm.Event, 0),
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

func (self *MockLocal) Terminate() {
    self.Mock.Called()
}

func (self *MockLocal) QueryState() string {
    args := self.Mock.Called()
    return args.String(0)
}

func (self *MockLocal) GetCurrentTerm() uint64 {
    args := self.Mock.Called()
    return args.Uint64(0)
}

func (self *MockLocal) GetLocalAddr() ps.ServerAddr {
    args := self.Mock.Called()
    var s ps.ServerAddr
    var ok bool
    if s, ok = args.Get(0).(ps.ServerAddr); !ok {
        panic(fmt.Sprintf("argument: %s not correct type ServerAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) GetVotedFor() ps.ServerAddr {
    args := self.Mock.Called()
    var s ps.ServerAddr
    var ok bool
    if s, ok = args.Get(0).(ps.ServerAddr); !ok {
        panic(fmt.Sprintf("argument: %s not correct type ServerAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) GetLeader() ps.ServerAddr {
    args := self.Mock.Called()
    var s ps.ServerAddr
    var ok bool
    if s, ok = args.Get(0).(ps.ServerAddr); !ok {
        panic(fmt.Sprintf("argument: %s not correct type ServerAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) Log() ps.Log {
    return self.log
}

func (self *MockLocal) SnapshotManager() ps.SnapshotManager {
    return self.snapshotManager
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
    addr ps.ServerAddr,
    eventHandler func(ev.RaftRequestEvent)) *comm.MemoryServer {

    bindAddr := testServers[1]
    logger := logging.GetLogger("test server #" + bindAddr.String())
    server := comm.NewMemoryServer(&bindAddr, eventHandler, testRegister, logger)
    go server.Serve()
    return server
}

func getTestPeerAndLocal(eventHandler func(ev.RaftEvent)) (Peer, *MockLocal, error) {
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
    snapshotManager := ps.NewMemorySnapshotManager()
    configManager := ps.NewMemoryConfigManager(index, conf)
    notifier := NewNotifier()
    local := NewMockLocal(log, snapshotManager, configManager, notifier)
    client := comm.NewMemoryClient(DefaultPoolSize, testRegister)
    logger := logging.GetLogger("test peer")

    peer := NewPeerMan(
        HeartbeatTimeout,
        MaxTimeoutJitter,
        MaxAppendEntriesSize,
        MaxSnapshotChunkSize,
        servers[1],
        client,
        eventHandler,
        local,
        logger)
    return peer, local, nil
}

func getTestPeerAndLocalSafe(
    t *testing.T, eventHandler func(ev.RaftEvent)) (Peer, *MockLocal) {

    peer, local, err := getTestPeerAndLocal(eventHandler)
    assert.Nil(t, err)
    return peer, local
}

func TestPeer(_ *testing.T) {
    // TODO add impl
}
