package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestMajorityCommitCondition(t *testing.T) {
    slice := ps.RandomMemoryMultiAddrSlice(3)
    cond := NewMajorityCommitCondition(slice)
    // test IsInCluster()
    for _, server := range slice.AllMultiAddr() {
        assert.True(t, cond.IsInCluster(server))
    }
    addr := ps.RandomMemoryMultiAddr()
    assert.False(t, cond.IsInCluster(addr))

    // test AddVote()
    assert.Equal(t, cond.MajoritySize, 2)
    assert.Equal(t, cond.VoteCount, 0)
    assert.NotNil(t, cond.AddVote(addr))
    assert.Nil(t, cond.AddVote(slice.Addresses[0]))
    assert.Equal(t, cond.VoteCount, 1)
    assert.False(t, cond.IsCommitted())
    assert.Nil(t, cond.AddVote(slice.Addresses[1]))
    assert.Equal(t, cond.VoteCount, 2)
    assert.True(t, cond.IsCommitted())
    assert.NotNil(t, cond.AddVote(slice.Addresses[1]))
    assert.Equal(t, cond.VoteCount, 2)
    assert.Nil(t, cond.AddVote(slice.Addresses[2]))
    assert.Equal(t, cond.VoteCount, 3)
}

func TestMemberChangeCommitCondition(t *testing.T) {
    total := 8
    clusterSize := 5
    slice := ps.RandomMemoryMultiAddrSlice(total)
    oldServers := &ps.ServerAddressSlice{
        Addresses: slice.Addresses[0:clusterSize],
    }
    newServers := &ps.ServerAddressSlice{
        Addresses: slice.Addresses[total-clusterSize:],
    }
    conf := &ps.Config{
        Servers:    oldServers,
        NewServers: newServers,
    }
    cond := NewMemberChangeCommitCondition(conf)
    // test AddVote()
    addr := ps.RandomMemoryMultiAddr()
    assert.NotNil(t, cond.AddVote(addr))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 0)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 0)
    assert.Nil(t, cond.AddVote(oldServers.AllMultiAddr()[0]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 1)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 0)
    assert.Nil(t, cond.AddVote(newServers.AllMultiAddr()[clusterSize-1]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 1)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 1)
    assert.Nil(t, cond.AddVote(oldServers.AllMultiAddr()[total-clusterSize]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 2)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 2)
    assert.False(t, cond.IsCommitted())
    assert.Nil(t, cond.AddVote(oldServers.AllMultiAddr()[total-clusterSize+1]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 3)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 3)
    assert.True(t, cond.IsCommitted())
}

func TestInflightAdd(t *testing.T) {
    clusterSize := 5
    slice := ps.RandomMemoryMultiAddrSlice(clusterSize)
    conf := &ps.Config{
        Servers:    slice,
        NewServers: nil,
    }
    inflight := NewInflight(conf)
    // test Add()
    resultChan := make(chan ev.Event)
    logEntry := &ps.LogEntry{
        Term:  testTerm,
        Index: testIndex,
        Type:  ps.LogNoop,
        Data:  testData,
        Conf:  conf,
    }
    request := &InflightRequest{
        LogEntry:   logEntry,
        ResultChan: resultChan,
    }
    assert.Nil(t, inflight.Add(request))
    committedEntries := inflight.GetCommitted()
    assert.Empty(t, committedEntries)
    addr := slice.Addresses[0]
    good, err := inflight.Replicate(addr, 0)
    assert.NotNil(t, err)
    good, err = inflight.Replicate(addr, testIndex)
    assert.Nil(t, err)
    assert.False(t, good)
    good, err = inflight.Replicate(slice.AllMultiAddr()[1], testIndex)
    assert.Nil(t, err)
    assert.False(t, good)
    good, err = inflight.Replicate(slice.AllMultiAddr()[2], testIndex)
    assert.Nil(t, err)
    assert.True(t, good)
    committedEntries = inflight.GetCommitted()
    assert.Equal(t, 1, len(committedEntries))
    assert.Equal(t, request, committedEntries[0].Request)
}

func TestInflightAddAll(t *testing.T) {
    clusterSize := 3
    slice := ps.RandomMemoryMultiAddrSlice(clusterSize)
    conf := &ps.Config{
        Servers:    slice,
        NewServers: nil,
    }
    inflight := NewInflight(conf)
    // test AddAll()
    resultChan := make(chan ev.Event)
    logEntries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  testTerm,
            Index: testIndex,
            Type:  ps.LogCommand,
            Data:  testData,
            Conf:  conf,
        },
        &ps.LogEntry{
            Term:  testTerm + 1,
            Index: testIndex + 1,
            Type:  ps.LogNoop,
            Data:  testData,
            Conf:  conf,
        },
    }
    inflightEntries := make([]*InflightEntry, 0, len(logEntries))
    for _, entry := range logEntries {
        request := &InflightRequest{
            LogEntry:   entry,
            ResultChan: resultChan,
        }
        inflightEntry := NewInflightEntry(request)
        inflightEntries = append(inflightEntries, inflightEntry)
    }
    assert.Nil(t, inflight.AddAll(inflightEntries))
    committedEntries := inflight.GetCommitted()
    assert.Empty(t, committedEntries)
    addr1 := slice.AllMultiAddr()[0]
    addr2 := slice.AllMultiAddr()[1]
    good, err := inflight.Replicate(addr1, testIndex)
    assert.Nil(t, err)
    assert.False(t, good)
    good, err = inflight.Replicate(addr2, testIndex)
    assert.Nil(t, err)
    assert.True(t, good)
    committedEntries = inflight.GetCommitted()
    assert.Equal(t, 1, len(committedEntries))
    assert.Equal(t, inflightEntries[0], committedEntries[0])
    good, err = inflight.Replicate(addr1, testIndex+1)
    assert.Nil(t, err)
    assert.False(t, good)
    good, err = inflight.Replicate(addr2, testIndex+1)
    assert.Nil(t, err)
    assert.True(t, good)
    committedEntries = inflight.GetCommitted()
    assert.Equal(t, 1, len(committedEntries))
    assert.Equal(t, inflightEntries[1], committedEntries[0])
}

func TestInflightChangeMemeber(_ *testing.T) {
    // TODO add impl
}
