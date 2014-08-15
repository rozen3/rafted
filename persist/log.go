package persist

// LogType is used to describe different types of log entries.
type LogType uint8

// Differents types of log entries.
const (
    LogUnknown LogType = iota

    // LogCommand is applied to the FSM.
    LogCommand

    // LogNoop is used to ensure the leadership for leader.
    LogNoop

    // LogMemberChange is used for member change in the cluster.
    LogMemberChange

    // LogBarrier is used to ensure all preceeding operations have heen
    // applied to the FSM. A loan from hashicorp-raft.
    LogBarrier
)

// Configuration represents the serialization of membership of the cluster.
type Configuration struct {
    // There are three possible combinations of Servers and NewServers:
    // 1. Servers != nil, NewServers == nil
    //     It's not in member change procedure now. Servers contains
    //     all members of the cluster.
    // 2. Servers != nil, NewServers != nil
    //     It's in member change procedure phase 1. Servers contains
    //     all old members of the cluster, and NewServers is the new members.
    // 3. Servers == nil, NewServers != nil
    //     It's in member change procedure phase 2. NewServers contains
    //     all the new members of the cluster.
    Servers    [][]byte
    NewServers [][]byte
}

// LogEntry is the element of replicated log in raft.
type LogEntry struct {
    Term  uint64
    Index uint64
    Type  LogType
    Data  []byte
    Conf  *Configuration
}

// Log is the interface for local durable log in raft.
// It provides functions to store and retrieve LogEntry.
// Any implementation of this interface should ensure the duration.
type Log interface {
    // Returns the term of the first LogEntry written. 0 for no entry.
    FirstTerm() (uint64, error)

    // Returns the index of the first LogEntry written. 0 for no entry.
    FirstIndex() (uint64, error)

    // Returns the term and index of the first LogEntry written.
    // Both term and index are 0 if there is no entry.
    FirstEntryInfo() (term uint64, index uint64, err error)

    // Returns the term mof the last LogEntry written. 0 for no entry.
    LastTerm() (uint64, error)

    // Returns the index of the last LogEntry written. 0 for no entry.
    LastIndex() (uint64, error)

    // Returns the term and index of the last LogEntry written.
    // Both term and index are 0 if there is no entry.
    LastEntryInfo() (term uint64, index uint64, err error)

    // Returns the index of log entry latest committed
    CommittedIndex() (uint64, error)

    // Store the index of log entry latest committed
    StoreCommittedIndex(index uint64) error

    // Returns the index of log entry latest applied to state machine
    LastAppliedIndex() (uint64, error)

    // Store the index of log entry latest applied to state machine
    StoreLastAppliedIndex(index uint64) error

    // Gets a log entry at a given index
    GetLog(index uint64) (*LogEntry, error)

    // Gets all log entris in range
    GetLogInRange(fromIndex uint64, toIndex uint64) ([]*LogEntry, error)

    // Store a single log entry
    StoreLog(log *LogEntry) error

    // Store multiple log entries
    StoreLogs(logs []*LogEntry) error

    // Delete the log entries before and up to the given index,
    // including the log entry right at the index.
    TruncateBefore(index uint64) error

    // Delete the log entries after the given index,
    // including the log entry right at index.
    TruncateAfter(index uint64) error
}
