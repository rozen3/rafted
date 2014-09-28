package rafted

import (
    ev "github.com/zonas/rafted/event"
    ps "github.com/zonas/rafted/persist"
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
    // leader should be elected before 10 rounds
    timeout := testConfig.ElectionTimeout * 10
    notifyChan := nodes[0].GetNotifyChan()
    // wait for a leader to step up
Outermost:
    for {
        select {
        case event := <-notifyChan:
            assert.True(t, ev.IsNotifyEvent(event.Type()))
            if event.Type() == ev.EventNotifyLeaderChange {
                break Outermost
            }
        case <-time.After(timeout):
            require.True(t, false)
        }
    }
    // start to Append()
    data := testData
    result, err := nodes[0].Append(data)
    require.Equal(t, nil, err)
    require.Equal(t, data, result)
    // cleanup
    todo := make([]func(), len(nodes))
    for _, node := range nodes {
        f := func() {
            assert.Nil(t, node.Close())
        }
        todo = append(todo, f)
    }
    ParallelDo(todo)
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
