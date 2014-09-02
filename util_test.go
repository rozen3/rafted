package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
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
