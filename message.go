package rafted

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
type AppendEntriesRequest struct {
    // Provide the current term and leader
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
    // Provide the term and our id
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

// PrepareInstallSnapshotRequest is the command sent to a Raft peer to bootstrap its
// log (and state machine) from a snapshot on another peer.
type PrepareInstallSnapshotRequest struct {
    Term   uint64
    Leader []byte

    // These are the last index/term included in the snapshot
    LastLogIndex uint64
    LastLogTerm  uint64

    // Peer Set in the snapshot
    Peers []byte

    // Size of the snapshot
    Size int64
}

// PrepareInstallSnapshotResponse is the response of an PrepareInstallSnapshotRequest.
type PrepareInstallSnapshotResponse struct {
    Term    uint64
    Success bool
}

// InstallSnapshotRequest is the command to start tranfering the content of snapshot
type InstallSnapshotRequest struct {
    SnapshotID uint64
    SegmentID  uint64
    Segment    []byte
    CheckSum   []byte
    Size       uint64
}

// InstallSnapshotResponse is the response of an InstallSnapshotRequest.
type InstallSnapshotResponse struct {
    SnapshotID uint64
    SegmentID  uint64
    Success    bool
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
