package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/mock"
    "github.com/hhkbp2/testify/require"
    "testing"
)

func TestPeerHeartbeatTimeout(t *testing.T) {
    // TODO add impl
    assert.True(t, true)
}

func TestPeerActivate(t *testing.T) {
    peer, _ := getTestPeerAndLocalSafe(t)
    // test construction
    assert.Equal(t, StateDeactivatedPeerID, peer.QueryState())
    event := ev.NewPeerActivateEvent()
    peer.Send(event)
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    peer.Close()
}

func TestPeerCandidateEnterLeaderOnEnterLeaderEvent(t *testing.T) {
    peer, mockLocal := getTestPeerAndLocalSafe(t)
    requestChan := NewReliableEventChannel()
    requestHandler := func(event ev.RequestEvent) {
        requestChan.Send(event)
    }
    leaderAddr := testServers.Addresses[0]
    peerAddr := testServers.Addresses[1]
    server := getTestMemoryServer(peerAddr, requestHandler)

    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())

    mockLocal.On("GetCurrentTerm").Return(testTerm).Twice()
    mockLocal.On("GetLocalAddr").Return(leaderAddr).Once()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    assert.Equal(t, StateStandardModePeerID, peer.QueryState())

    peer.Close()
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
    requestHandler := func(event ev.RequestEvent) {
        requestChan.Send(event)
        event.SendResponse(respEvent)
    }
    leaderAddr := testServers.Addresses[0]
    peerAddr := testServers.Addresses[1]
    server := getTestMemoryServer(peerAddr, requestHandler)

    peer, mockLocal := getTestPeerAndLocalSafe(t)
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
    assertGetAppendEntriesRequestEvent(t, requestChan.GetOutChan(), 0, request)

    peer.Close()
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
    requestHandler := func(event ev.RequestEvent) {
        requestChan.Send(event)
        event.SendResponse(respEvent)
    }
    leaderAddr := testServers.Addresses[0]
    peerAddr := testServers.Addresses[1]
    server := getTestMemoryServer(peerAddr, requestHandler)

    peer, mockLocal := getTestPeerAndLocalSafe(t)
    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    mockLocal.On("GetCurrentTerm").Return(testTerm)
    mockLocal.On("GetLocalAddr").Return(leaderAddr)
    mockLocal.On("SendPrior", mock.Anything).Return()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    assert.Equal(t, StateLeaderPeerID, peer.QueryState())
    // three times are enough
    request := &ev.AppendEntriesRequest{
        Term:              testTerm,
        Leader:            leaderAddr,
        LeaderCommitIndex: testIndex,
    }
    nchan := mockLocal.Notifier().GetNotifyChan()
    assertGetHeartbeatTimeoutNotify(t, nchan, testConfig.HeartbeatTimeout)
    assertGetAppendEntriesRequestEvent(t, requestChan.GetOutChan(), 0, request)
    assertGetHeartbeatTimeoutNotify(t, nchan, testConfig.HeartbeatTimeout)
    assertGetAppendEntriesRequestEvent(t, requestChan.GetOutChan(), 0, request)
    assertGetHeartbeatTimeoutNotify(t, nchan, testConfig.HeartbeatTimeout)
    assertGetAppendEntriesRequestEvent(t, requestChan.GetOutChan(), 0, request)
    assert.Equal(t, 1, len(mockLocal.PriorEvents))

    peer.Close()
    assert.Nil(t, server.Close())
}

func TestPeerStandardModeCatchingUp(t *testing.T) {
    logger := logging.GetLogger("abc")
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    requestCount := 0
    peerLogIndex := testIndex
    // a simple simulation for a peer which accept one log entry on every AE rpc
    requestHandler := func(event ev.RequestEvent) {
        assert.Equal(t, ev.EventAppendEntriesRequest, event.Type(),
            "expect %s but actual %s",
            ev.EventTypeString(ev.EventAppendEntriesRequest),
            ev.EventTypeString(event.Type()))
        e, ok := event.(*ev.AppendEntriesRequestEvent)
        assert.True(t, ok)
        logger.Debug("** requestCount: %d, peerLogIndex: %d", requestCount, peerLogIndex)
        requestCount++
        var response *ev.AppendEntriesResponse
        if e.Request.PrevLogIndex == peerLogIndex {
            lastIndex := e.Request.PrevLogIndex + uint64(len(e.Request.Entries))
            if peerLogIndex < lastIndex {
                peerLogIndex++
            }
            response = &ev.AppendEntriesResponse{
                Term:         e.Request.Term,
                LastLogIndex: peerLogIndex,
                Success:      true,
            }
        } else {
            response = &ev.AppendEntriesResponse{
                Term:         e.Request.Term,
                LastLogIndex: peerLogIndex,
                Success:      false,
            }
        }
        logger.Debug("** peer response with, term: %d, LastLogIndex: %d, Success: %t", response.Term, response.LastLogIndex, response.Success)
        event.SendResponse(ev.NewAppendEntriesResponseEvent(response))
    }
    leaderAddr := testServers.Addresses[0]
    peerAddr := testServers.Addresses[1]
    server := getTestMemoryServer(peerAddr, requestHandler)
    // get peer
    peer, mockLocal := getTestPeerAndLocalSafe(t)
    // add a few log entry ahead in leader's log
    aheadCount := 3
    for i := 1; i <= aheadCount; i++ {
        entry := &ps.LogEntry{
            Term:  testTerm,
            Index: testIndex + uint64(i),
            Type:  ps.LogCommand,
            Data:  testData,
            Conf: &ps.Config{
                Servers:    testServers,
                NewServers: nil,
            },
        }
        assert.Nil(t, mockLocal.Log().StoreLog(entry))
    }

    peer.Send(ev.NewPeerActivateEvent())
    assert.Equal(t, StateCandidatePeerID, peer.QueryState())
    mockLocal.On("GetCurrentTerm").Return(testTerm)
    mockLocal.On("GetLocalAddr").Return(leaderAddr)
    mockLocal.On("SendPrior", mock.Anything).Return()
    peer.Send(ev.NewPeerEnterLeaderEvent())
    nchan := mockLocal.Notifier().GetNotifyChan()
    assertGetHeartbeatTimeoutNotify(t, nchan, testConfig.HeartbeatTimeout)
    heartbeatCount := 3
    for i := 0; i < aheadCount+heartbeatCount; i++ {
        assertGetHeartbeatTimeoutNotify(t, nchan, testConfig.HeartbeatTimeout)
    }
    assert.Equal(t, StateLeaderPeerID, peer.QueryState())
    assert.True(t, requestCount >= aheadCount+1+heartbeatCount)
    assert.Equal(t, aheadCount+1, len(mockLocal.PriorEvents))
    for i := 0; i <= aheadCount; i++ {
        event := mockLocal.PriorEvents[i]
        assert.Equal(t, ev.EventPeerReplicateLog, event.Type(),
            "expect %s but actual %s",
            ev.EventTypeString(ev.EventPeerReplicateLog),
            ev.EventTypeString(event.Type()))
        e, ok := event.(*ev.PeerReplicateLogEvent)
        assert.True(t, ok)
        assert.Equal(t, peerAddr, e.Message.Peer)
        assert.Equal(t, testIndex+uint64(i), e.Message.MatchIndex)
    }

    peer.Close()
    assert.Nil(t, server.Close())
}
