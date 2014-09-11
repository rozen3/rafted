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

var (
    testMockPeersRequests []ev.RaftRequestEvent
)

func getLocalAndPeersForLeader(t *testing.T) (Local, *MockPeers) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    local, peers := getTestLocalAndPeersForCandidate(t)
    time.Sleep(ElectionTimeout)
    nchan := local.Notifier().GetNotifyChan()
    SwallowNotifyNow(t, nchan, 3)
    assert.Equal(t, StateCandidateID, local.QueryState())
    peers.On("Broadcast", mock.Anything).Return().Once()
    term := testTerm + 1
    lastLogIndex, err := local.Log().LastIndex()
    assert.Nil(t, err)
    committedIndex, err := local.Log().CommittedIndex()
    assert.Nil(t, err)
    logEntries, err := local.Log().GetLogInRange(
        Min(committedIndex+1, lastLogIndex), lastLogIndex)
    assert.Nil(t, err)
    conf, err := local.ConfigManager().RNth(0)
    assert.Nil(t, err)
    logEntry := &ps.LogEntry{
        Term:  term,
        Index: lastLogIndex + 1,
        Type:  ps.LogNoop,
        Data:  make([]byte, 0),
        Conf:  conf,
    }
    logEntries = append(logEntries, logEntry)
    lastLogTerm, err := local.Log().LastTerm()
    assert.Nil(t, err)
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            local.GetLocalAddr(),
        PrevLogIndex:      lastLogIndex,
        PrevLogTerm:       lastLogTerm,
        Entries:           logEntries,
        LeaderCommitIndex: committedIndex,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    peers.On("Broadcast", mock.Anything).Return().Once()
    testMockPeersRequests = append(testMockPeersRequests, reqEvent)
    response := &ev.RequestVoteResponse{
        Term:    term,
        Granted: true,
    }
    respEvent := ev.NewRequestVoteResponseEvent(response)
    voter := testServers[2]
    respEvent.FromAddr = voter
    local.Send(respEvent)
    SwallowNotify(t, nchan, ElectionTimeout, 2)
    assert.Equal(t, StateUnsyncID, local.QueryState())
    assert.Equal(t, term, local.GetCurrentTerm())
    return local, peers
}

func getLocalAndPeersForSync(t *testing.T) (Local, *MockPeers) {
    local, peers := getLocalAndPeersForLeader(t)
    event := testMockPeersRequests[0]
    reqEvent, ok := event.(*ev.AppendEntriesRequestEvent)
    assert.True(t, ok)
    request := reqEvent.Request
    follower := testServers[1]
    peerUpdate := &ev.PeerReplicateLog{
        Peer:       follower,
        MatchIndex: request.PrevLogIndex + 1,
    }
    peerEvent := ev.NewPeerReplicateLogEvent(peerUpdate)
    local.Send(peerEvent)
    nchan := local.Notifier().GetNotifyChan()
    assertGetCommitNotify(t, nchan, ElectionTimeout,
        request.Term, request.PrevLogIndex+1)
    assertGetApplyNotify(t, nchan, 0, request.Term, request.PrevLogIndex+1)
    assert.Equal(t, StateSyncID, local.QueryState())
    return local, peers
}

func TestLeaderUnsyncHandlePeerUpdate(t *testing.T) {
    local, _ := getLocalAndPeersForLeader(t)
    event := testMockPeersRequests[0]
    reqEvent, ok := event.(*ev.AppendEntriesRequestEvent)
    assert.True(t, ok)
    request := reqEvent.Request
    // ensure log status
    assertLogLastIndex(t, local.Log(), request.PrevLogIndex+1)
    assertLogLastTerm(t, local.Log(), request.Term)
    assertLogCommittedIndex(t, local.Log(), request.LeaderCommitIndex)
    // dispatch peer update event
    follower := testServers[1]
    peerUpdate := &ev.PeerReplicateLog{
        Peer:       follower,
        MatchIndex: request.PrevLogIndex + 1,
    }
    peerEvent := ev.NewPeerReplicateLogEvent(peerUpdate)
    local.Send(peerEvent)
    nchan := local.Notifier().GetNotifyChan()
    assertGetCommitNotify(t, nchan, ElectionTimeout,
        request.Term, request.PrevLogIndex+1)
    assert.Equal(t, StateSyncID, local.QueryState())
    assert.Equal(t, request.Term, local.GetCurrentTerm())
    // check log status
    assertLogLastIndex(t, local.Log(), request.PrevLogIndex+1)
    assertLogLastTerm(t, local.Log(), request.Term)
    assertLogCommittedIndex(t, local.Log(), request.PrevLogIndex+1)
    //
    local.Terminate()
}

func TestLeaderHandleAppendEntriesRequest(t *testing.T) {
    local, _ := getLocalAndPeersForLeader(t)
    // test handling older term request
    leader := testServers[0]
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  testTerm + 1,
            Index: testIndex + 1,
            Type:  ps.LogCommand,
            Data:  testData,
            Conf: &ps.Config{
                Servers:    testServers,
                NewServers: nil,
            },
        },
    }
    request := &ev.AppendEntriesRequest{
        Term:              testTerm + 1,
        Leader:            leader,
        PrevLogIndex:      testIndex,
        PrevLogTerm:       testTerm,
        Entries:           entries,
        LeaderCommitIndex: testIndex,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(
        t, reqEvent, false, testTerm+1, testIndex+1)
    // test handling newer term request
    nextTerm := testTerm + 2
    nextIndex := testIndex + 2
    entries = []*ps.LogEntry{
        &ps.LogEntry{
            Term:  nextTerm,
            Index: nextIndex,
            Type:  ps.LogNoop,
            Data:  testData,
            Conf: &ps.Config{
                Servers:    testServers,
                NewServers: nil,
            },
        },
    }
    request = &ev.AppendEntriesRequest{
        Term:              nextTerm,
        Leader:            leader,
        PrevLogIndex:      nextIndex - 1,
        PrevLogTerm:       nextTerm - 1,
        Entries:           entries,
        LeaderCommitIndex: nextIndex - 1,
    }
    reqEvent = ev.NewAppendEntriesRequestEvent(request)
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, true, nextTerm, nextIndex)
    nchan := local.Notifier().GetNotifyChan()
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateLeader, ev.RaftStateFollower)
    assertGetTermChangeNotify(t, nchan, 0, nextTerm-1, nextTerm)
    assertGetLeaderChangeNotify(t, nchan, 0, leader)
    assertGetCommitNotify(t, nchan, 0, nextTerm-1, nextIndex-1)
    assertGetApplyNotify(t, nchan, 0, nextTerm-1, nextIndex-1)
    //
    local.Terminate()
}

func TestLeaderHandleRequestVoteRequest(t *testing.T) {
    local, _ := getLocalAndPeersForSync(t)
    // test handling older term request
    candidate := testServers[1]
    request := &ev.RequestVoteRequest{
        Term:         testTerm + 1,
        Candidate:    candidate,
        LastLogIndex: testIndex,
        LastLogTerm:  testTerm,
    }
    reqEvent := ev.NewRequestVoteRequestEvent(request)
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, false, testTerm+1)
    // test handling newer term request
    nextTerm := testTerm + 2
    nextIndex := testIndex + 2
    request = &ev.RequestVoteRequest{
        Term:         nextTerm,
        Candidate:    candidate,
        LastLogIndex: nextIndex - 1,
        LastLogTerm:  nextTerm - 1,
    }
    reqEvent = ev.NewRequestVoteRequestEvent(request)
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, true, nextTerm)
    nchan := local.Notifier().GetNotifyChan()
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateLeader, ev.RaftStateFollower)
    assertGetTermChangeNotify(t, nchan, 0, nextTerm-1, nextTerm)
    //
    local.Terminate()
}

func TestLeaderHandleInstallSnapshotRequest(_ *testing.T) {
    // TODO add impl
}

func TestLeaderUnsyncHandleClientReadOnlyRequest(t *testing.T) {
    local, _ := getLocalAndPeersForLeader(t)
    request := &ev.ClientReadOnlyRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    local.Send(reqEvent)
    assertGetLeaderUnsyncResponseEvent(t, reqEvent)
    //
    local.Terminate()
}

func TestLeaderSyncHandleClientReadOnlyRequest(t *testing.T) {
    local, peers := getLocalAndPeersForSync(t)
    // dispatch the client request
    peers.On("Broadcast", mock.Anything).Return().Once()
    request := &ev.ClientReadOnlyRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    local.Send(reqEvent)
    // dispatch the peer update
    follower := testServers[2]
    peerUpdate := &ev.PeerReplicateLog{
        Peer:       follower,
        MatchIndex: testIndex + 2,
    }
    peerEvent := ev.NewPeerReplicateLogEvent(peerUpdate)
    local.Send(peerEvent)
    nchan := local.Notifier().GetNotifyChan()
    assertGetCommitNotify(t, nchan, ElectionTimeout, testTerm+1, testIndex+2)
    assertGetApplyNotify(t, nchan, ElectionTimeout, testTerm+1, testIndex+2)
    assertGetClientResponseEvent(t, reqEvent, true, testData)
    //
    local.Terminate()
}

func TestLeaderSyncHandleClientAppendRequest(t *testing.T) {
    local, peers := getLocalAndPeersForSync(t)
    // dispatch the client request
    peers.On("Broadcast", mock.Anything).Return().Once()
    request := &ev.ClientAppendRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    local.Send(reqEvent)
    // dispatch the peer update
    follower := testServers[1]
    peerUpdate := &ev.PeerReplicateLog{
        Peer:       follower,
        MatchIndex: testIndex + 2,
    }
    peerEvent := ev.NewPeerReplicateLogEvent(peerUpdate)
    local.Send(peerEvent)
    nchan := local.Notifier().GetNotifyChan()
    assertGetCommitNotify(t, nchan, ElectionTimeout, testTerm+1, testIndex+2)
    assertGetApplyNotify(t, nchan, ElectionTimeout, testTerm+1, testIndex+2)
    assertGetClientResponseEvent(t, reqEvent, true, testData)
    //
    local.Terminate()
}
