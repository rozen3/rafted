package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/rafted/str"
    "github.com/stretchr/testify/assert"
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

// func TestApplier(t *testing.T) {
// }

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
