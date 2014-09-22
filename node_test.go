package rafted

import (
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestNode(t *testing.T) {
    clusterSize := 3
    addrs := ps.SetupMemoryServerAddrs(clusterSize)
    nodes := make([]*RaftNode, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := NewTestHSMBackend(addrs[i], addrs)
        if err != nil {
            t.Error("fail to create test raft node, error")
        }
        client := setupTestRedirectClient(addrs[i], backend)
        node := NewRaftNode(backend, client)
        nodes = append(nodes, node)
    }
    data := testData
    result, err := nodes[0].Append(data)
    assert.Equal(t, nil, err)
    assert.Equal(t, data, result)
}
