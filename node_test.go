package rafted

import (
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
    "time"
)

func testNodeSimple(
    t *testing.T,
    genBackend GenHSMBackendFunc,
    genRedirectHandler GenRedirectClientFunc) {

    clusterSize := 3
    addrs := ps.SetupMemoryServerAddrs(clusterSize)
    nodes := make([]*RaftNode, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := genBackend(addrs[i], addrs)
        assert.Nil(t, err, "fail to create test backend")
        client, err := genRedirectHandler(addrs[i], backend)
        assert.Nil(t, err, "fail to create test client")
        node := NewRaftNode(backend, client)
        nodes = append(nodes, node)
    }
    time.Sleep(testConfig.ElectionTimeout)
    data := testData
    result, err := nodes[0].Append(data)
    assert.Equal(t, nil, err)
    assert.Equal(t, data, result)
    // cleanup
    for _, node := range nodes {
        assert.Nil(t, node.Close())
    }
}

func TestMemoryNodeSimple(t *testing.T) {
    testNodeSimple(t, NewTestMemoryHSMBackend, setupTestMemoryRedirectClient)
}

func TestSocketNodeSimple(t *testing.T) {
    testNodeSimple(t, NewTestSocketHSMBackend, setupTestSocketRedirectClient)
}
