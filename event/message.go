package event

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
type AppendEntriesRequest struct {
    // Provide the current term and leader ID
    Term   uint64
    Leader []byte

    // Provide the previous entries for integrity checking
    PrevLogIndex uint64
    PrevLogTerm  uint64

    // New entries to commit
    Entries []*LogEntry

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
    Candidate []byte

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
    Leader []byte

    // the snapshot replaces all entries up through and including this index
    LastIncludedIndex uint64
    // term of lastIncludedIndex
    LastLogTerm uint64

    // byte offset where chunk is positioned in the snapshot file
    Offset uint64

    // raw bytes of the snapshot chunk, starting at offset
    Data []byte

    // The configuration of all servers on LastIncludedIndex log entry
    // (when taking snapshot)
    Servers []byte

    // Size of the snapshot
    Size int64
}

// InstallSnapshotResponse is the response of an InstallSnapshotRequest.
type InstallSnapshotResponse struct {
    // current term of the follower, for leader to update itself
    Term uint64
}

// LeaderRedirectResponse is to redirect client to leader.
type LeaderRedirectResponse struct {
    // The network addr of leader
    LeaderAddr string
}

// LeaderUnknownResponse is to tell client we are not leader and
// don't known which node in cluter is leader.
type LeaderUnknownResponse struct {
}
