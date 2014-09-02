package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    "testing"
    "time"
)

var (
    testData         = []byte(str.RandomString(100))
    testIndex uint64 = 100
    testTerm  uint64 = 10
)

func getTestLog(
    committedIndex, lastAppliedIndex uint64,
    entry *ps.LogEntry) (ps.Log, error) {

    log := ps.NewMemoryLog()
    if err := log.StoreLog(entry); err != nil {
        return nil, err
    }
    if err := log.StoreCommittedIndex(committedIndex); err != nil {
        return nil, err
    }
    if err := log.StoreLastAppliedIndex(lastAppliedIndex); err != nil {
        return nil, err
    }
    return log, nil
}

func getTestLocal() (*Local, error) {
    servers := ps.SetupMemoryServerAddrs(3)
    localAddr := servers[0]
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
        MaxTimeoutJitter,
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

func NewMockPeers(local *Local) *MockPeers {
    object := &MockPeers{}
    local.SetPeers(object)
    return object
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
    NewMockPeers(local)
    assert.Equal(t, local.GetCurrentTerm(), testTerm)
    stateID := local.QueryState()
    assert.Equal(t, StateFollowerID, stateID)
    //time.Sleep(ElectionTimeout)
    //assert.Equal(t, StateCandidateID, stateID)
    nchan := local.Notifier().GetNotifyChan()
    select {
    case e := <-nchan:
        assert.Equal(t, e.Type(), ev.EventNotifyElectionTimeout)
    case <-time.After(time.Hour * 1):
        assert.True(t, false)
    }
}
