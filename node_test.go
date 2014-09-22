package rafted

import (
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "testing"
    "time"
)

func testNodeSimple(
    t *testing.T,
    backendAddrs []ps.ServerAddr,
    clientAddrs []ps.ServerAddr,
    genBackendFunc GenHSMBackendFunc,
    genRedirectFunc GenRedirectClientFunc) {

    clusterSize := len(backendAddrs)
    nodes := make([]*RaftNode, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := genBackendFunc(backendAddrs[i], backendAddrs)
        require.Nil(t, err, "fail to create test backend")
        client, err := genRedirectFunc(clientAddrs[i], backend)
        require.Nil(t, err, "fail to create test client")
        node := NewRaftNode(backend, client)
        nodes = append(nodes, node)
    }
    time.Sleep(testConfig.ElectionTimeout)
    data := testData
    result, err := nodes[0].Append(data)
    require.Equal(t, nil, err)
    require.Equal(t, data, result)
    // cleanup
    for _, node := range nodes {
        assert.Nil(t, node.Close())
    }
}

func TestMemoryNodeSimple(t *testing.T) {
    clusterSize := 3
    addrs := ps.SetupSocketServerAddrs(clusterSize * 2)
    testNodeSimple(
        t, addrs[:clusterSize], addrs[clusterSize:],
        NewTestMemoryHSMBackend, setupTestMemoryRedirectClient)
}

func TestSocketNodeSimple(t *testing.T) {
    clusterSize := 3
    addrs := ps.SetupSocketServerAddrs(clusterSize * 2)
    testNodeSimple(
        t, addrs[:clusterSize], addrs[clusterSize:],
        NewTestSocketHSMBackend, setupTestSocketRedirectClient)
}
