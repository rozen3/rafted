package rafted

import (
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestMajorityCommitCondition(t *testing.T) {
    servers := ps.RandomMemoryServerAddrs(3)
    commitCondition := NewMajorityCommitCondition(servers)
    // test IsInCluster()
    for _, server := range servers {
        assert.True(t, commitCondition.IsInCluster(server))
    }
    addr := ps.RandomMemoryServerAddr()
    assert.False(t, commitCondition.IsInCluster(addr))

    // test AddVote()
    assert.Equal(t, commitCondition.MajoritySize, 2)
    assert.Equal(t, commitCondition.VoteCount, 0)
    assert.NotNil(t, commitCondition.AddVote(addr))
    assert.Nil(t, commitCondition.AddVote(servers[0]))
    assert.Equal(t, commitCondition.VoteCount, 1)
    assert.False(t, commitCondition.IsCommitted())
    assert.Nil(t, commitCondition.AddVote(servers[1]))
    assert.Equal(t, commitCondition.VoteCount, 2)
    assert.True(t, commitCondition.IsCommitted())
    assert.NotNil(t, commitCondition.AddVote(servers[1]))
    assert.Equal(t, commitCondition.VoteCount, 2)
    assert.Nil(t, commitCondition.AddVote(servers[2]))
    assert.Equal(t, commitCondition.VoteCount, 3)
}
