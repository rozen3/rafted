package rafted

// LogType is used to describe different types of log entries.
type LogType uint8

// Differents types of log entries.
const (
    // LogCommand is applied to the FSM.
    LogCommand LogType = iota

    // LogNoop is used to ensure the leadership for leader.
    LogNoop

    // LogAddrPeer is used to add a new node for the cluster.
    LogAddrPeer

    // LogRemovePeer is used to remove a node for the cluster.
    LogRemovePeer

    // LogBarrier is used to ensure all preceeding operations have heen
    // applied to the FSM. A loan from hashicorp-raft.
    LogBarrier
)

// LogEntry is the element of replicated log in raft.
type LogEntry struct {
    Term  uint64
    Index uint64
    Type  LogType
    Data  []byte
}

// Log is the interface for local durable log in raft.
// It provides functions to store and retrieve LogEntry.
// Any implementation of this interface should ensure the duration.
type Log interface {
    // Returns the index of the first LogEntry written. 0 for no entries.
    FirstIndex() (uint64, error)

    // Returns the term of the first LogEntry written. 0 for no entries.
    FirstTerm() (uint64, error)

    // Returns the index of the last LogEntry written. 0 for no entries.
    LastIndex() (uint64, error)

    // Returns the term mof the last LogEntry written. 0 for no entries.
    LastTerm() (uint64, error)

    // Gets a log entry at a given index
    GetLog(index uint64) (*LogEntry, error)

    // Store a single log entry
    StoreLog(log *LogEntry) error

    // Store multiple log entries
    StoreLogs(logs []*LogEntry) error

    // Delete the log entries before and up to the given index,
    // including the log entry right at the index.
    TruncateBefore(index uint64) error

    // Delete the log entries after the given index,
    // not including the log entry right at index.
    TruncateAfter(index uint64) error
}
