package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "testing"
    "time"
)

func testNodeSimple(
    t *testing.T,
    genBackendFunc GenHSMBackendFunc,
    genRedirectFunc GenRedirectClientFunc) {

    clusterSize := 3
    slice := ps.SetupSocketMultiAddrSlice(clusterSize * 2)
    backendAddrSlice := &ps.ServerAddressSlice{
        Addresses: slice.Addresses[:clusterSize],
    }
    clientAddrs := slice.Addresses[clusterSize:]
    nodes := make([]*RaftNode, 0, clusterSize)
    for i := 0; i < clusterSize; i++ {
        backend, err := genBackendFunc(
            backendAddrSlice.Addresses[i], backendAddrSlice)
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
    todo := make([]func(), 0, len(nodes))
    for _, node := range nodes {
        n := node
        f := func() {
            assert.Nil(t, n.Close())
        }
        todo = append(todo, f)
    }
    ParallelDo(todo)
}

func TestMemoryNodeSimple(t *testing.T) {
    testNodeSimple(t, NewTestMemoryHSMBackend, setupTestMemoryRedirectClient)
}

func TestSocketNodeSimple(t *testing.T) {
    testNodeSimple(t, NewTestSocketHSMBackend, setupTestSocketRedirectClient)
}

func TestRPCNodeSimple(t *testing.T) {
    testNodeSimple(t, NewTestRPCHSMBackend, setupTestRPCRediectClient)
}
