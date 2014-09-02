package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    "testing"
)

var (
    testData = []byte(str.RandomString(100))
)

func getTestLocal() (*Local, error) {
    servers := ps.SetupMemoryServerAddrs(3)
    localAddr := servers[0]
    index := uint64(100)
    term := uint64(10)
    log := ps.NewMemoryLog()
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
    if err := log.StoreLog(entry); err != nil {
        return nil, err
    }
    if err := log.StoreCommittedIndex(index); err != nil {
        return nil, err
    }
    if err := log.StoreLastAppliedIndex(index); err != nil {
        return nil, err
    }
    stateMachine := ps.NewMemoryStateMachine()
    snapshotManager := ps.NewMemorySnapshotManager()
    configManager := ps.NewMemoryConfigManager(index, conf)
    logger := logging.GetLogger("test local")
    local, err := NewLocal(
        HeartbeatTimeout,
        ElectionTimeout,
        ElectionTimeoutThresholdPersent,
        PersistErrorNotifyTimeout,
        localAddr,
        log,
        stateMachine,
        snapshotManager,
        configManager,
        logger)
    if err != nil {
        return nil, err
    }
    return local, nil
}

type MockPeers struct {
    mock.Mock
}

func (self *MockPeers) Broadcast(event hsm.Event) {
    self.Mock.Called(event)
}

func (self *MockPeers) AddPeers(peerAddrs []ps.ServerAddr) {
    self.Mock.Called(peerAddrs)
}

func (self *MockPeers) RemovePeers(peerAddrs []ps.ServerAddr) {
    self.Mock.Called(peerAddrs)
}

func TestLocal(t *testing.T) {
    local, err := getTestLocal()
    assert.Nil(t, err)
    peers := new(MockPeers)
    local.SetPeers(peers)
    t.Log(local.GetCurrentTerm())
}
