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

func getTestLocalAndPeersForCandidate(t *testing.T) (*Local, *MockPeers) {
    local, peers := getTestLocalAndPeers(t)

    peers.On("Broadcast", mock.Anything).Return().Once()
    peers.On("AddPeers", mock.Anything).Return().Once()
    peers.On("Broadcast", mock.Anything).Return().Once()
    peers.On("Broadcast", mock.Anything).Return().Once()
    //    peers.On("Broadcast", mock.Anything).Return().Once()

    // peers.On("Broadcast", mock.Anything).Return()
    // peers.On("AddPeers", mock.Anything).Return()

    return local, peers
}

func TestCandidateElectionTimeout(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    local, _ := getTestLocalAndPeersForCandidate(t)
    time.Sleep(ElectionTimeout)
    // check election timeout for follower -> candidate
    nchan := local.Notifier().GetNotifyChan()
    assertGetElectionTimeoutNotify(t, nchan, 0)
    // check state change notify
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateFollower, ev.RaftStateCandidate)
    // check term change notify
    nextTerm := testTerm + 1
    assertGetTermChangeNotify(t, nchan, 0, testTerm, nextTerm)
    assert.Equal(t, StateCandidateID, local.QueryState())
    assert.Equal(t, nextTerm, local.GetCurrentTerm())
    // check election timeout for candidate -> candidate
    assertGetElectionTimeoutNotify(t, nchan, ElectionTimeout)
    assert.Equal(t, StateCandidateID, local.QueryState())
    assert.Equal(t, nextTerm+1, local.GetCurrentTerm())
    //
    local.Terminate()
}

func TestCandidateHandleAppendEntriesRequest(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    local, _ := getTestLocalAndPeersForCandidate(t)
    time.Sleep(ElectionTimeout)
    // check election timeout for follower -> candidate
    nchan := local.Notifier().GetNotifyChan()
    assertGetElectionTimeoutNotify(t, nchan, 0)
    // check state change notify
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateFollower, ev.RaftStateCandidate)
    // check term change notify
    nextTerm := testTerm + 1
    assertGetTermChangeNotify(t, nchan, 0, testTerm, nextTerm)
    assert.Equal(t, StateCandidateID, local.QueryState())
    assert.Equal(t, nextTerm, local.GetCurrentTerm())
    // handle stale term request
    leader := testServers[1]
    term := testTerm
    nextIndex := testIndex + 1
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  term,
            Index: nextIndex,
            Type:  ps.LogCommand,
            Data:  testData,
            Conf: &ps.Config{
                Servers:    testServers,
                NewServers: nil,
            },
        },
    }
    request := &ev.AppendEntriesRequest{
        Term:              term,
        Leader:            leader,
        PrevLogIndex:      testIndex,
        PrevLogTerm:       term,
        Entries:           entries,
        LeaderCommitIndex: testIndex,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    assert.Equal(t, StateCandidateID, local.QueryState())
    local.Dispatch(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, false, nextTerm, testIndex)
    assert.Equal(t, StateCandidateID, local.QueryState())
    // test new term request
    nextTerm += 1
    request.Term = nextTerm
    request.Entries[0].Term = nextTerm
    reqEvent = ev.NewAppendEntriesRequestEvent(request)
    local.Dispatch(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, true, nextTerm, testIndex)
    // check notifies
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateCandidate, ev.RaftStateFollower)
    assertGetTermChangeNotify(t, nchan, 0, nextTerm-1, nextTerm)
    assertGetLeaderChangeNotify(t, nchan, 0, leader)
    // check internal status
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, leader, local.GetLeader())
    assert.Equal(t, StateFollowerID, local.QueryState())
    //
    local.Terminate()
}

func TestCandidateHandleClientRequest(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    local, _ := getTestLocalAndPeersForCandidate(t)
    time.Sleep(ElectionTimeout)
    // ignore 3 notifies
    nchan := local.Notifier().GetNotifyChan()
    SwallowNotifyNow(t, nchan, 3)
    // handle client request
    request := &ev.ClientAppendRequest{
        Data: testData,
    }
    reqEvent := ev.NewClientAppendRequestEvent(request)
    assert.Equal(t, StateCandidateID, local.QueryState())
    local.Dispatch(reqEvent)
    assertGetLeaderUnknownResponse(t, reqEvent)
    //
    local.Terminate()
}
