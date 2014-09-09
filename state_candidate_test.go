package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
    //ps "github.com/hhkbp2/rafted/persist"
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
    //peers.On("Broadcast", mock.Anything).Return().Once()
    return local, peers
}

func TestCandidateElectionTimeout(t *testing.T) {
    require.Nil(t, assert.SetCallerInfoLevelNumber(3))
    local, _ := getTestLocalAndPeersForCandidate(t)
    time.Sleep(ElectionTimeout)
    // check election timeout for follower -> candidate
    nchan := local.Notifier().GetNotifyChan()
    assertGetElectionTimeoutNotify(t, nchan, 0)
    assertGetStateChangeNotify(t, nchan, 0,
        ev.RaftStateFollower, ev.RaftStateCandidate)
    //    assertGetApplyNotify(t, nchan, 0, testTerm, testIndex)
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
