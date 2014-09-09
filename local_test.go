package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/mock"
    "testing"
    "time"
)

var (
    testData           = []byte(str.RandomString(100))
    testIndex   uint64 = 100
    testTerm    uint64 = 10
    testServers        = ps.SetupMemoryServerAddrs(3)
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
    servers := testServers
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

func BeforeTimeout(timeout time.Duration, startTime time.Time) time.Duration {
    d := time.Duration(
        int64(float32(int64(timeout)) * (1 - MaxTimeoutJitter)))
    return (d - time.Now().Sub(startTime))
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

func getTestLocalSafe(t *testing.T) *Local {
    local, err := getTestLocal()
    assert.Nil(t, err)
    return local
}

func getTestLocalAndPeers(t *testing.T) (*Local, *MockPeers) {
    local := getTestLocalSafe(t)
    return local, NewMockPeers(local)
}

func assertGetRequestVoteResponseEvent(
    t *testing.T, reqEvent ev.RaftRequestEvent, granted bool, term uint64) {

    respEvent := reqEvent.RecvResponse()
    assert.Equal(t, ev.EventRequestVoteResponse, respEvent.Type())
    event, ok := respEvent.(*ev.RequestVoteResponseEvent)
    assert.True(t, ok)
    assert.Equal(t, granted, event.Response.Granted)
    assert.Equal(t, term, event.Response.Term)
}

func assertGetAppendEntriesResponseEvent(t *testing.T,
    reqEvent ev.RaftRequestEvent, success bool, term, index uint64) {

    respEvent := reqEvent.RecvResponse()
    assert.Equal(t, ev.EventAppendEntriesResponse, respEvent.Type())
    event, ok := respEvent.(*ev.AppendEntriesResponseEvent)
    assert.True(t, ok)
    assert.Equal(t, success, event.Response.Success)
    assert.Equal(t, term, event.Response.Term)
    assert.Equal(t, index, event.Response.LastLogIndex)
}

func assertGetElectionTimeoutNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration) {

    select {
    case e := <-notifyChan:
        assert.Equal(t, ev.EventNotifyElectionTimeout, e.Type())
    case <-time.After(afterTime):
        assert.True(t, false)
    }
}

func assertNotGetElectionTimeoutNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration) {

    select {
    case <-notifyChan:
        assert.True(t, false)
    case <-time.After(afterTime):
        // Do nothing
    }
}

func assertGetStateChangeNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration,
    oldState, newState ev.RaftStateType) {

    select {
    case event := <-notifyChan:
        assert.Equal(t, ev.EventNotifyStateChange, event.Type())
        e, ok := event.(*ev.NotifyStateChangeEvent)
        assert.True(t, ok)
        assert.Equal(t, oldState, e.OldState)
        assert.Equal(t, newState, e.NewState)
    case <-time.After(afterTime):
        assert.True(t, false)
    }
}

func assertGetLeaderChangeNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration,
    leader ps.ServerAddr) {

    select {
    case event := <-notifyChan:
        assert.Equal(t, ev.EventNotifyLeaderChange, event.Type())
        e, ok := event.(*ev.NotifyLeaderChangeEvent)
        assert.True(t, ok)
        assert.Equal(t, leader, e.NewLeader)
    case <-time.After(afterTime):
        assert.True(t, false)
    }
}

func assertGetTermChangeNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration,
    oldTerm, newTerm uint64) {

    select {
    case event := <-notifyChan:
        assert.Equal(t, event.Type(), ev.EventNotifyTermChange)
        e, ok := event.(*ev.NotifyTermChangeEvent)
        assert.True(t, ok)
        assert.Equal(t, oldTerm, e.OldTerm)
        assert.Equal(t, newTerm, e.NewTerm)
    case <-time.After(afterTime):
        assert.True(t, false)
    }
}

func assertGetApplyNotify(
    t *testing.T, notifyChan <-chan ev.NotifyEvent, afterTime time.Duration,
    term, index uint64) {

    select {
    case event := <-notifyChan:
        assert.Equal(t, ev.EventNotifyApply, event.Type())
        e, ok := event.(*ev.NotifyApplyEvent)
        assert.True(t, ok)
        assert.Equal(t, term, e.Term)
        assert.Equal(t, index, e.LogIndex)
    case <-time.After(afterTime):
        assert.True(t, false)
    }
}

func assertLogLastIndex(t *testing.T, log ps.Log, index uint64) {
    lastLogIndex, err := log.LastIndex()
    assert.Nil(t, err)
    assert.Equal(t, index, lastLogIndex)
}

func assertLogLastTerm(t *testing.T, log ps.Log, term uint64) {
    lastTerm, err := log.LastTerm()
    assert.Nil(t, err)
    assert.Equal(t, term, lastTerm)
}

func assertLogCommittedIndex(t *testing.T, log ps.Log, index uint64) {
    committedIndex, err := log.CommittedIndex()
    assert.Nil(t, err)
    assert.Equal(t, index, committedIndex)
}
