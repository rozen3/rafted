package event

import (
    ps "github.com/hhkbp2/rafted/persist"
    "time"
)

// ------------------------------------------------------------
// Raft Messages
// ------------------------------------------------------------

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
type AppendEntriesRequest struct {
    // Provide the current term and leader ID
    Term   uint64
    Leader ps.ServerAddr

    // Provide the previous entries for integrity checking
    PrevLogIndex uint64
    PrevLogTerm  uint64

    // New entries to commit
    Entries []*ps.LogEntry

    // Commit index on the leader
    LeaderCommitIndex uint64
}

// AppendEntriesResponse is the response returned from an
// AppendEntriesRequest.
type AppendEntriesResponse struct {
    // Newer term if leader is out of date
    Term uint64

    // Last log index is a hint to help accelerate rebuilding slow nodes
    LastLogIndex uint64

    // We may not succeed if we have a conflicting entry
    Success bool
}

// RequestVoteRequest is the command used by a candidate to ask a Raft peer
// for a vote in an election.
type RequestVoteRequest struct {
    // Provide the term and our ID
    Term      uint64
    Candidate ps.ServerAddr

    // Used to ensure safety
    LastLogIndex uint64
    LastLogTerm  uint64
}

// RequestVoteResponse is the response returned from a RequestVoteRequest.
type RequestVoteResponse struct {
    // Newer term if leader is out of date
    Term uint64

    // Is the vote granted
    Granted bool
}

// InstallSnapshotRequest is the request sent to a Raft follower
// whose log is too far behind to catch up with leader.
// Snapshot is sent to the follower in chunks from the leader.
type InstallSnapshotRequest struct {
    // Leader's current term
    Term uint64
    // Leader ID
    Leader ps.ServerAddr

    // the snapshot replaces all entries up through and including this index
    LastIncludedIndex uint64
    // term of lastIncludedIndex
    LastIncludedTerm uint64

    // byte offset where chunk is positioned in the snapshot file
    Offset uint64

    // raw bytes of the snapshot chunk, starting at offset
    Data []byte

    // The configuration of all servers on LastIncludedIndex log entry
    // (when taking snapshot)
    Conf *ps.Config

    // Size of the snapshot
    Size uint64
}

// InstallSnapshotResponse is the response of an InstallSnapshotRequest.
type InstallSnapshotResponse struct {
    // current term of the follower, for leader to update itself
    Term uint64
    // whether the request is accepted by the follower
    Success bool
}

// ------------------------------------------------------------
// Client Messages
// ------------------------------------------------------------

// ClientAppendRequest is the general request to be appended to raft log.
type ClientAppendRequest struct {
    // the request content, to be applied to state machine
    Data []byte
}

// ClientReadOnlyRequest is a read-only request not to be appended to raft log.
type ClientReadOnlyRequest struct {
    // the request content, to be applied to state machine
    Data []byte
}

// ClientMemberChangeRequest is a request for a member change in cluster.
type ClientMemberChangeRequest struct {
    Conf *ps.Config
}

// ClientResponse is a general response of raft module answer to client.
type ClientResponse struct {
    // whether the request handling is a success or failure.
    Success bool
    // the response content
    Data []byte
}

// LeaderRedirectResponse contains the leader info for client to redirect.
type LeaderRedirectResponse struct {
    // The network address of leader
    Leader ps.ServerAddr
}

// ------------------------------------------------------------
// Internal Messages
// ------------------------------------------------------------

type QueryStateResponse struct {
    StateID string
}

// Timeout is a message contains timeout info for all kinds of timeout
// in this raft module, which includes heartbeat timeout and election timeout.
type Timeout struct {
    LastTime time.Time
    Timeout  time.Duration
}

// PeerReplicateLog is a internal message for a peer(which represents
// a follower) to signal leader it makes some progress on log replication.
type PeerReplicateLog struct {
    // network addr of the peer(follower)
    Peer ps.ServerAddr
    // index of highest log entry known to replicated to follower
    MatchIndex uint64
}

// MemberChangeNewConf contains new configuration of the cluster.
// It is used in member change prodedure for follower state.
type MemberChangeNewConf struct {
    Conf *ps.Config
}

type LeaderForwardMemberChangePhase struct {
    Conf       *ps.Config
    ResultChan chan RaftEvent
}
