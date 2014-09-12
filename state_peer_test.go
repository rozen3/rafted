package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestPeerHeartbeatTimeout(t *testing.T) {
    // TODO add impl
    assert.True(t, true)
}

func TestPeerActivate(t *testing.T) {
    responseHandler := func(event ev.RaftEvent) {
        // empty body
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

func TestPeerCandidateEnterLeaderOnEnterLeaderEvent(t *testing.T) {
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
    //
    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    //
    mockLocal.On("GetCurrentTerm").Return(testTerm).Twice()
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Once()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    assert.Equal(t, StateStandardModePeerID, peer.QueryState())
    //
    peer.Terminate()
    assert.Nil(t, server.Close())
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
    //
    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    //
    mockLocal.On("GetCurrentTerm").Return(testTerm).Twice()
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Once()
    nextTerm := testTerm + 1
    nextIndex := testIndex + 1
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  nextTerm,
            Index: nextIndex,
            Type:  ps.LogNoop,
            Data:  make([]byte, 0),
            Conf: &ps.Config{
                Servers:    testServers,
                NewServers: nil,
            },
        },
    }
    request := &ev.AppendEntriesRequest{
        Term:              nextTerm,
        Leader:            leaderAddr,
        PrevLogIndex:      nextIndex - 1,
        PrevLogTerm:       nextTerm - 1,
        Entries:           entries,
        LeaderCommitIndex: nextIndex - 1,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    peer.Send(reqEvent)
    assert.Equal(t, StateStandardModePeerID, peer.QueryState())
    //
    peer.Terminate()
    assert.Nil(t, server.Close())
}
