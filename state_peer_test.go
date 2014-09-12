package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestPeerHeartbeatTimeout(t *testing.T) {
    // TODO add impl
    assert.True(t, true)
}

func TestPeerActivate(t *testing.T) {
    // TODO add impl
    responseHandler := func(event ev.RaftEvent) {
        // TODO empty body
    }
    peer, _ := getTestPeerAndLocalSafe(t, responseHandler)
    // test construction
    assert.Equal(t, StateDeactivatedPeerID, peer.QueryState())
    event := ev.NewPeerActivateEvent()
    peer.Send(event)
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    //
    peer.Terminate()
}

func TestPeerCandidateEnterLeaderOnAppendEntriesRequest(t *testing.T) {
    responseChan := NewReliableEventChannel()
    responseHandler := func(event ev.RaftEvent) {
        responseChan.Send(event)
    }
    peer, mockLocal := getTestPeerAndLocalSafe(t, responseHandler)
    requestChan := NewReliableEventChannel()
    requestHandler := func(event ev.RaftRequestEvent) {
        requestChan.Send(event)
    }
    leaderAddr := testServers[0]
    peerAddr := testServers[1]
    server := getTestMemoryServer(peerAddr, requestHandler)
    //getTestMemoryServer(peerAddr, requestHandler)
    //
    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    //
    mockLocal.On("GetCurrentTerm").Return(testTerm).Twice()
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Once()
    //mockLocal.On("GetCurrentTerm").Return(uint64(1))
    peer.Send(ev.NewPeerEnterLeaderEvent())
    //assert.Equal(t, StateLeaderPeerID, peer.QueryState())
    assert.Equal(t, StateStandardModePeerID, peer.QueryState())
    //
    peer.Terminate()
    assert.Nil(t, server.Close())
}
