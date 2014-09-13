package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/mock"
    "github.com/hhkbp2/testify/require"
    "testing"
    "time"
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

    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())

    mockLocal.On("GetCurrentTerm").Return(testTerm).Twice()
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Once()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    assert.Equal(t, StateStandardModePeerID, peer.QueryState())

    peer.Terminate()
    assert.Nil(t, server.Close())
}

func TestPeerCandidateEnterLeaderOnAppendEntriesRequest(t *testing.T) {
    requestChan := NewReliableEventChannel()
    nextTerm := testTerm + 1
    nextIndex := testIndex + 1
    response := &ev.AppendEntriesResponse{
        Term:         nextTerm,
        LastLogIndex: nextIndex,
        Success:      true,
    }
    respEvent := ev.NewAppendEntriesResponseEvent(response)
    requestHandler := func(event ev.RaftRequestEvent) {
        requestChan.Send(event)
        event.SendResponse(respEvent)
    }
    leaderAddr := testServers[0]
    peerAddr := testServers[1]
    server := getTestMemoryServer(peerAddr, requestHandler)

    responseChan := NewReliableEventChannel()
    responseHandler := func(event ev.RaftEvent) {
        responseChan.Send(event)
    }
    peer, mockLocal := getTestPeerAndLocalSafe(t, responseHandler)

    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())

    mockLocal.On("GetCurrentTerm").Return(nextTerm).Times(3)
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Twice()
    mockLocal.On("SendPrior", mock.Anything).Return()
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
    assert.Equal(t, StateLeaderPeerID, peer.QueryState())
    select {
    case event := <-requestChan.GetOutChan():
        assert.Equal(t, ev.EventAppendEntriesRequest, event.Type(),
            "expect %s, but actual %s",
            ev.EventTypeString(ev.EventAppendEntriesRequest),
            ev.EventTypeString(event.Type()))
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        assert.True(t, ok)
        assert.Equal(t, request.Term, e.Request.Term)
    case <-time.After(HeartbeatTimeout):
        assert.True(t, false)
    }

    peer.Terminate()
    assert.Nil(t, server.Close())
}

func TestPeerLeaderHeartbeatTimeout(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    requestChan := NewReliableEventChannel()
    response := &ev.AppendEntriesResponse{
        Term:         testTerm,
        LastLogIndex: testIndex,
        Success:      true,
    }
    respEvent := ev.NewAppendEntriesResponseEvent(response)
    requestHandler := func(event ev.RaftRequestEvent) {
        requestChan.Send(event)
        event.SendResponse(respEvent)
    }
    leaderAddr := testServers[0]
    peerAddr := testServers[1]
    server := getTestMemoryServer(peerAddr, requestHandler)

    responseHandler := func(_ ev.RaftEvent) {
        // empty body
    }
    peer, mockLocal := getTestPeerAndLocalSafe(t, responseHandler)

    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    mockLocal.On("GetCurrentTerm").Return(testTerm)
    mockLocal.On("GetLocalAddr").Return(leaderAddr)
    mockLocal.On("SendPrior", mock.Anything).Return()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    assert.Equal(t, StateLeaderPeerID, peer.QueryState())
    nchan := mockLocal.Notifier().GetNotifyChan()
    // three times are enough
    assertGetHeartbeatTimeoutNotify(t, nchan, HeartbeatTimeout)
    assertGetHeartbeatTimeoutNotify(t, nchan, HeartbeatTimeout)
    assertGetHeartbeatTimeoutNotify(t, nchan, HeartbeatTimeout)

    peer.Terminate()
    assert.Nil(t, server.Close())
}
