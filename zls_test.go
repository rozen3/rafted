package rafted

import (
	//ps "github.com/zonas/rafted/persist"
	"github.com/hhkbp2/testify/require"
	"github.com/hhkbp2/testify/assert"
	"github.com/hhkbp2/testify/mock"
	"testing"
	"time"
)

func TestFollowerElectionTimeout1(t *testing.T) {
	require.Nil(t, assert.SetCallerInfoLevelNumber(2))
	local, peers := getTestLocalAndPeers(t)
	peers.On("Broadcast", mock.Anything).Return().Once()
	peers.On("AddPeers", mock.Anything).Return().Once()
	peers.On("Broadcast", mock.Anything).Return().Once()
	peers.On("Broadcast", mock.Anything).Return().Once()
	// check initial term and state
	assert.Equal(t, local.GetCurrentTerm(), testTerm)
	assert.Equal(t, StateFollowerID, local.QueryState())
	// check election timeout
	nchan := local.Notifier().GetNotifyChan()
	assertGetElectionTimeoutNotify(t, nchan, testConfig.ElectionTimeout)
	// check state transfer after timeout
	assert.Equal(t, StateCandidateID, local.QueryState())

	time.Sleep(1 * time.Second)
	peers.On("Broadcast", mock.Anything).Return().Times(3)
	assert.Equal(t, StateLeaderID, local.QueryState())
	local.Close()
}
