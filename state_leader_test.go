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

func getLocalAndPeersForLeader(t *testing.T) (*Local, *MockPeers) {
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
    local.Dispatch(respEvent)
    SwallowNotify(t, nchan, ElectionTimeout, 2)
    assert.Equal(t, StateUnsyncID, local.QueryState())
    assert.Equal(t, term, local.GetCurrentTerm())
    return local, peers
}

func TestLeaderUnsyncHandleClientReadOnlyRequest(t *testing.T) {
    local, _ := getLocalAndPeersForLeader(t)
    request := &ev.ClientReadOnlyRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientReadOnlyRequestEvent(request)
    local.Dispatch(reqEvent)
    assertGetLeaderUnsyncResponseEvent(t, reqEvent)
    //
    local.Terminate()
}

func TestLeaderUnsyncHandleAppendEntriesResponse(t *testing.T) {
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
    local.Dispatch(peerEvent)
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
