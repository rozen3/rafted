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

func TestFollowerElectionTimeout(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(2))
    local, peers := getTestLocalAndPeers(t)
    peers.On("Broadcast", mock.AnythingOfType("*event.PeerActivateEvent")).
        Return().Once()
    peers.On("AddPeers", mock.AnythingOfType("[]persist.ServerAddr")).
        Return().Once()
    peers.On("Broadcast", mock.AnythingOfType("*event.RequestVoteRequestEvent")).
        Return().Once()
    peers.On("Broadcast", mock.AnythingOfType("*event.PeerDeactivateEvent")).
        Return().Once()
    // check initial term and state
    assert.Equal(t, local.GetCurrentTerm(), testTerm)
    assert.Equal(t, StateFollowerID, local.QueryState())
    // check election timeout
    nchan := local.Notifier().GetNotifyChan()
    assertGetElectionTimeoutNotify(t, nchan, ElectionTimeout)
    // check state transfer after timeout
    assert.Equal(t, StateCandidateID, local.QueryState())
    local.Terminate()
}

func TestFollowerHandleRequestVoteRequest(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(2))
    local := getTestLocalSafe(t)
    // check ignore old term request
    candidate := testServers[1]
    request := &ev.RequestVoteRequest{
        Term:         testTerm - 1,
        Candidate:    candidate,
        LastLogIndex: testIndex + 10,
        LastLogTerm:  testTerm + 1,
    }
    reqEvent := ev.NewRequestVoteRequestEvent(request)
    assert.Equal(t, StateFollowerID, local.QueryState())
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, false, testTerm)
    // test handle new term request
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, ps.NilServerAddr, local.GetLeader())
    newTerm := testTerm + 1
    request.Term = newTerm
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, true, newTerm)
    // check notify term change
    nchan := local.Notifier().GetNotifyChan()
    assertGetTermChangeNotify(t, nchan, 0, testTerm, newTerm)
    // check internal status
    assert.Equal(t, candidate, local.GetVotedFor())
    assert.Equal(t, ps.NilServerAddr, local.GetLeader())
    // check the state remains
    assert.Equal(t, StateFollowerID, local.QueryState())
    // test duplicated request
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, true, newTerm)
    // test corrupted request
    newTerm += 1
    candidate = testServers[2]
    request = &ev.RequestVoteRequest{
        Term:         newTerm,
        Candidate:    candidate,
        LastLogIndex: testIndex - 10,
        LastLogTerm:  testTerm - 1,
    }
    reqEvent = ev.NewRequestVoteRequestEvent(request)
    startTime := time.Now()
    local.Send(reqEvent)
    assertGetRequestVoteResponseEvent(t, reqEvent, false, newTerm)
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, ps.NilServerAddr, local.GetLeader())
    // still get term change notify
    assertGetTermChangeNotify(t, nchan, 0, newTerm-1, newTerm)
    // test election timeout ticker reset on RV
    assertNotGetElectionTimeoutNotify(t, nchan,
        BeforeTimeout(ElectionTimeout, startTime))
    //
    local.Terminate()
}

func TestFollowerHandleAppendEntriesRequest(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(2))
    local := getTestLocalSafe(t)
    // check ignore old term request
    leader := testServers[1]
    nextTerm := testTerm + 1
    nextIndex := testIndex + 1
    entries := []*ps.LogEntry{
        &ps.LogEntry{
            Term:  nextTerm,
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
        Term:              testTerm - 1,
        Leader:            leader,
        PrevLogIndex:      testIndex,
        PrevLogTerm:       testTerm,
        Entries:           entries,
        LeaderCommitIndex: testIndex,
    }
    reqEvent := ev.NewAppendEntriesRequestEvent(request)
    assert.Equal(t, StateFollowerID, local.QueryState())
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, false, testTerm, testIndex)
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, ps.NilServerAddr, local.GetLeader())
    // test new term request
    request.Term = nextTerm
    reqEvent = ev.NewAppendEntriesRequestEvent(request)
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, true, nextTerm, testIndex)
    // check notify term change
    nchan := local.Notifier().GetNotifyChan()
    assertGetTermChangeNotify(t, nchan, 0, testTerm, nextTerm)
    assertGetLeaderChangeNotify(t, nchan, 0, leader)
    // check internal status
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, leader, local.GetLeader())
    // check state remains
    assert.Equal(t, StateFollowerID, local.QueryState())
    // check log status
    assertLogLastIndex(t, local.Log(), nextIndex)
    assertLogLastTerm(t, local.Log(), nextTerm)
    assertLogCommittedIndex(t, local.Log(), testIndex)
    // test duplicate request
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, true, nextTerm, nextIndex)
    // test corrupted request
    nextTerm += 1
    leader = testServers[2]
    request = &ev.AppendEntriesRequest{
        Term:              nextTerm,
        Leader:            leader,
        PrevLogIndex:      testIndex,
        PrevLogTerm:       testTerm - 1,
        Entries:           entries,
        LeaderCommitIndex: testIndex,
    }
    reqEvent = ev.NewAppendEntriesRequestEvent(request)
    startTime := time.Now()
    local.Send(reqEvent)
    assertGetAppendEntriesResponseEvent(t, reqEvent, false, nextTerm, nextIndex)
    assertGetTermChangeNotify(t, nchan, 0, nextTerm-1, nextTerm)
    assertGetLeaderChangeNotify(t, nchan, 0, leader)
    assert.Equal(t, ps.NilServerAddr, local.GetVotedFor())
    assert.Equal(t, leader, local.GetLeader())
    // test election timeout ticker reset on this AP
    assertNotGetElectionTimeoutNotify(t, nchan,
        BeforeTimeout(ElectionTimeout, startTime))
    //
    local.Terminate()
}
