package rafted

import (
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestMajorityCommitCondition(t *testing.T) {
    servers := ps.RandomMemoryServerAddrs(3)
    cond := NewMajorityCommitCondition(servers)
    // test IsInCluster()
    for _, server := range servers {
        assert.True(t, cond.IsInCluster(server))
    }
    addr := ps.RandomMemoryServerAddr()
    assert.False(t, cond.IsInCluster(addr))

    // test AddVote()
    assert.Equal(t, cond.MajoritySize, 2)
    assert.Equal(t, cond.VoteCount, 0)
    assert.NotNil(t, cond.AddVote(addr))
    assert.Nil(t, cond.AddVote(servers[0]))
    assert.Equal(t, cond.VoteCount, 1)
    assert.False(t, cond.IsCommitted())
    assert.Nil(t, cond.AddVote(servers[1]))
    assert.Equal(t, cond.VoteCount, 2)
    assert.True(t, cond.IsCommitted())
    assert.NotNil(t, cond.AddVote(servers[1]))
    assert.Equal(t, cond.VoteCount, 2)
    assert.Nil(t, cond.AddVote(servers[2]))
    assert.Equal(t, cond.VoteCount, 3)
}

func TestMemberChangeCommitCondition(t *testing.T) {
    total := 8
    clusterSize := 5
    servers := ps.RandomMemoryServerAddrs(total)
    oldServers := servers[0:clusterSize]
    newServers := servers[total-clusterSize:]
    conf := &ps.Config{
        Servers:    oldServers,
        NewServers: newServers,
    }
    cond := NewMemberChangeCommitCondition(conf)
    // test AddVote()
    addr := ps.RandomMemoryServerAddr()
    assert.NotNil(t, cond.AddVote(addr))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 0)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 0)
    assert.Nil(t, cond.AddVote(oldServers[0]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 1)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 0)
    assert.Nil(t, cond.AddVote(newServers[clusterSize-1]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 1)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 1)
    assert.Nil(t, cond.AddVote(oldServers[total-clusterSize]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 2)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 2)
    assert.False(t, cond.IsCommitted())
    assert.Nil(t, cond.AddVote(oldServers[total-clusterSize+1]))
    assert.Equal(t, cond.OldServersCommitCondition.VoteCount, 3)
    assert.Equal(t, cond.NewServersCommitCondition.VoteCount, 3)
    assert.True(t, cond.IsCommitted())
}
