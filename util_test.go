package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    "io"
    "testing"
    "time"
)

func TestReliableEventChannel(t *testing.T) {
    ch := NewReliableEventChannel()
    // test Send()
    event1 := ev.NewStepdownEvent()
    ch.Send(event1)
    // test GetInChan()
    inChan := ch.GetInChan()
    event2 := ev.NewLeaderMemberChangeActivateEvent()
    inChan <- event2
    // test GetOutChan()
    outChan := ch.GetOutChan()
    select {
    // don't use default: label for chan readable condition
    case event := <-outChan:
        assert.Equal(t, event1, event)
    case <-time.After(0):
        assert.True(t, false)
    }
    // test Recv()
    event := ch.Recv()
    assert.Equal(t, event2, event)
    // test Close()
    ch.Close()
}

func TestReliableUint64Channel(t *testing.T) {
    ch := NewReliableUint64Channel()
    // test Send()
    v1 := uint64(98)
    ch.Send(v1)
    // test GetInChan()
    inChan := ch.GetInChan()
    v2 := uint64(3991034)
    inChan <- v2
    // test GetOutChan()
    outChan := ch.GetOutChan()
    select {
    case v := <-outChan:
        assert.Equal(t, v1, v)
    case <-time.After(0):
        assert.True(t, false)
    }
    // test Recv()
    v := ch.Recv()
    assert.Equal(t, v, v2)
    // test Close()
    ch.Close()
}

func TestReliableInflightEntryChannel(t *testing.T) {
    ch := NewReliableInflightEntryChannel()
    // test Send()
    request := &InflightRequest{
        LogEntry: &ps.LogEntry{
            Conf: &ps.Config{
                Servers: ps.SetupMemoryServerAddrs(3),
            },
        },
        ResultChan: make(chan ev.RaftEvent),
    }
    entry1 := NewInflightEntry(request)
    ch.Send(entry1)
    // test GetInChan()
    entry2 := NewInflightEntry(request)
    inChan := ch.GetInChan()
    inChan <- entry2
    // test GetOutChan()
    outChan := ch.GetOutChan()
    select {
    case e := <-outChan:
        assert.Equal(t, entry1, e)
    case <-time.After(0):
        assert.True(t, false)
    }
    // test Recv()
    e := ch.Recv()
    assert.Equal(t, entry2, e)
    // test Close()
    ch.Close()
}

func TestNotifier(t *testing.T) {
    notifier := NewNotifier()
    stopChan := make(chan int)
    term := testTerm
    index := testIndex
    event := ev.NewNotifyApplyEvent(term, index)
    notifyCount := 0
    go func() {
        // test GetNotifyChan()
        ch := notifier.GetNotifyChan()
        for {
            select {
            case <-stopChan:
                return
            case notify := <-ch:
                assert.Equal(t, notify.Type(), ev.EventNotifyApply)
                e, ok := notify.(*ev.NotifyApplyEvent)
                assert.True(t, ok)
                assert.Equal(t, term, e.Term)
                assert.Equal(t, index, e.LogIndex)
                notifyCount++
            }
        }
    }()
    // test Notify()
    notifier.Notify(event)
    notifier.Notify(event)
    stopChan <- 1
    assert.Equal(t, 2, notifyCount)
    // test Close()
    notifier.Close()
}

func TestClientEventListener(t *testing.T) {
    listener := NewClientEventListener()
    fnCount := 0
    response := &ev.ClientResponse{
        Success: true,
        Data:    []byte(str.RandomString(50)),
    }
    times := 2
    endCh := make(chan int)
    reqEvent := ev.NewClientResponseEvent(response)
    // test Start()
    fn := func(event ev.RaftEvent) {
        assert.Equal(t, ev.EventClientResponse, event.Type())
        e, ok := event.(*ev.ClientResponseEvent)
        assert.True(t, ok)
        assert.Equal(t, reqEvent, e)
        fnCount++
        if fnCount >= times {
            endCh <- 0
        }
    }
    listener.Start(fn)
    // test GetChan()
    ch := listener.GetChan()
    ch <- reqEvent
    ch <- reqEvent
    <-endCh
    assert.Equal(t, times, fnCount)
    // test Close()
    listener.Stop()
}

func argUint64(args mock.Arguments, index int) uint64 {
    var s uint64
    var ok bool
    if s, ok = args.Get(index).(uint64); !ok {
        panic(fmt.Sprintf("assert: arguemnts: Uint64(%d) failed "+
            "because object wasn't correct type: %s", index, args.Get(index)))
    }
    return s
}

type MockLog struct {
    mock.Mock
    logger logging.Logger
}

func NewMockLog(logger logging.Logger) *MockLog {
    return &MockLog{
        logger: logger,
    }
}

func (self *MockLog) FirstTerm() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) FirstIndex() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) FirstEntryInfo() (term uint64, index uint64, err error) {
    args := self.Mock.Called()
    return argUint64(args, 0), argUint64(args, 1), args.Error(2)
}

func (self *MockLog) LastTerm() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) LastIndex() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) LastEntryInfo() (term uint64, index uint64, err error) {
    args := self.Mock.Called()
    return argUint64(args, 0), argUint64(args, 1), args.Error(2)
}

func (self *MockLog) CommittedIndex() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) StoreCommittedIndex(index uint64) error {
    args := self.Mock.Called(index)
    return args.Error(0)
}

func (self *MockLog) LastAppliedIndex() (uint64, error) {
    args := self.Mock.Called()
    return argUint64(args, 0), args.Error(1)
}

func (self *MockLog) StoreLastAppliedIndex(index uint64) error {
    args := self.Mock.Called(index)
    return args.Error(0)
}

func (self *MockLog) GetLog(index uint64) (*ps.LogEntry, error) {
    args := self.Mock.Called(index)
    var entry *ps.LogEntry
    var ok bool
    if entry, ok = args.Get(0).(*ps.LogEntry); !ok {
        panic("fail because object isn't *LogEntry")
    }
    return entry, args.Error(1)
}

func (self *MockLog) GetLogInRange(
    fromIndex uint64, toIndex uint64) ([]*ps.LogEntry, error) {

    args := self.Mock.Called(fromIndex, toIndex)
    var entries []*ps.LogEntry
    var ok bool
    if entries, ok = args.Get(0).([]*ps.LogEntry); !ok {
        panic("fail because object isn't []*LogEntry")
    }
    return entries, args.Error(1)
}

func (self *MockLog) StoreLog(log *ps.LogEntry) error {
    args := self.Mock.Called(log)
    return args.Error(0)
}

func (self *MockLog) StoreLogs(logs []*ps.LogEntry) error {
    args := self.Mock.Called(logs)
    return args.Error(0)
}

func (self *MockLog) TruncateBefore(index uint64) error {
    args := self.Mock.Called(index)
    return args.Error(0)
}

func (self *MockLog) TruncateAfter(index uint64) error {
    args := self.Mock.Called(index)
    return args.Error(0)
}

type MockSnapshot struct {
    mock.Mock
}

func NewMockSnapshot() *MockSnapshot {
    return &MockSnapshot{}
}

func (self *MockSnapshot) Persist(writer ps.SnapshotWriter) error {
    args := self.Mock.Called(writer)
    return args.Error(0)
}

func (self *MockSnapshot) Release() {
    self.Mock.Called()
}

type MockStateMachine struct {
    mock.Mock
}

func NewMockStateMachine() *MockStateMachine {
    return &MockStateMachine{}
}

func (self *MockStateMachine) Apply(data []byte) []byte {
    args := self.Mock.Called(data)
    var s []byte
    var ok bool
    if s, ok = args.Get(0).([]byte); !ok {
        panic("fail because object isn't []byte")
    }
    return s
}

func (self *MockStateMachine) MakeSnapshot() (ps.Snapshot, error) {
    args := self.Mock.Called()
    var snapshot ps.Snapshot
    var ok bool
    if snapshot, ok = args.Get(0).(ps.Snapshot); !ok {
        panic("fail because object isn't Snapshot")
    }
    return snapshot, args.Error(1)
}

func (self *MockStateMachine) Restore(reader io.ReadCloser) error {
    args := self.Mock.Called(reader)
    return args.Error(0)
}

func TestApplierConstruction(t *testing.T) {
    lastAppliedIndex := uint64(10)
    committedIndex := uint64(12)
    number := int(committedIndex - lastAppliedIndex)
    logger := logging.GetLogger("test")
    log := NewMockLog(logger)
    log.On("CommittedIndex").Return(committedIndex, nil).Twice()
    log.On("LastAppliedIndex").Return(lastAppliedIndex, nil).Once()
    term := uint64(9)
    data := testData
    for i := lastAppliedIndex + 1; i <= committedIndex; i++ {
        entry := &ps.LogEntry{
            Term:  term,
            Index: i,
            Type:  ps.LogCommand,
            Data:  data,
            Conf: &ps.Config{
                Servers:    ps.RandomMemoryServerAddrs(5),
                NewServers: nil,
            },
        }
        log.On("GetLog", uint64(i)).Return(entry, nil).Once()
        log.On("StoreLastAppliedIndex", i).Return(nil).Once()
    }
    stateMachine := NewMockStateMachine()
    stateMachine.On("Apply", mock.AnythingOfType("[]uint8")).Return(
        []byte(""), nil).Times(number)
    dispatchCount := 0
    dispatcher := func(_ hsm.Event) {
        dispatchCount++
    }
    notifier := NewNotifier()

    stopChan := make(chan int)
    waitChan := make(chan int)
    notifyCount := 0
    go func() {
        ch := notifier.GetNotifyChan()
        for {
            select {
            case <-stopChan:
                return
            case event := <-ch:
                assert.Equal(t, ev.EventNotifyApply, event.Type())
                e, ok := event.(*ev.NotifyApplyEvent)
                assert.True(t, ok)
                assert.Equal(t, term, e.Term)
                index := lastAppliedIndex + uint64(notifyCount) + 1
                assert.Equal(t, index, e.LogIndex)
                notifyCount++
                if notifyCount >= number {
                    waitChan <- 1
                }
            }
        }
    }()
    applier := NewApplier(
        log, stateMachine, dispatcher, notifier, logger)
    <-waitChan
    stopChan <- 0
    assert.Equal(t, 0, dispatchCount)
    assert.Equal(t, number, notifyCount)
    applier.Close()
    notifier.Close()
}

func TestMin(t *testing.T) {
    v1 := uint64(5)
    v2 := uint64(6)
    m1 := Min(v1, v2)
    assert.Equal(t, m1, v1)
    m2 := Min(v2, v1)
    assert.Equal(t, m2, v1)
}

func TestMax(t *testing.T) {
    v1 := uint64(0)
    v2 := uint64(100)
    m1 := Max(v1, v2)
    assert.Equal(t, m1, v2)
    m2 := Max(v2, v1)
    assert.Equal(t, m2, v2)
}

func TestGetPeers(t *testing.T) {
    size := 10
    servers := ps.SetupMemoryServerAddrs(size)
    localAddr := servers[0]
    conf := &ps.Config{
        Servers:    servers[:5],
        NewServers: servers[5:],
    }
    peers := GetPeers(localAddr, conf)
    assert.Equal(t, len(servers)-1, len(peers))
    for _, peerAddr := range peers {
        assert.True(t, ps.AddrNotEqual(&localAddr, &peerAddr))
    }
}

func TestAddrsToMap(t *testing.T) {
    size := 10
    addrs := ps.RandomMemoryServerAddrs(size)
    m := AddrsToMap(addrs)
    assert.Equal(t, size, len(m))
    for _, v := range m {
        assert.Nil(t, v)
    }
}

func TestMapSetMinus(t *testing.T) {
    size := 10
    m1 := AddrsToMap(ps.RandomMemoryServerAddrs(size))
    m2 := AddrsToMap(ps.RandomMemoryServerAddrs(size))
    addrs := MapSetMinus(m1, m2)
    assert.Equal(t, size, len(addrs))
    for _, addr := range addrs {
        _, ok := m1[addr]
        assert.True(t, ok)
        _, ok = m2[addr]
        assert.False(t, ok)
    }
    m2[addrs[0]] = m1[addrs[0]]
    m2[addrs[3]] = m1[addrs[3]]
    addrs = MapSetMinus(m1, m2)
    assert.Equal(t, size-2, len(addrs))
    for _, addr := range addrs {
        _, ok := m1[addr]
        assert.True(t, ok)
        _, ok = m2[addr]
        assert.False(t, ok)
    }
}
