package raft_example

type LogType uint8

const (
    //
    LogCommand LogType = iota

    //
    LogNoop
    //
    LogAddrPeer
    //
    LogRemovePeer
    //
    LogBarrier
)

type LogEntry struct {
    Term  uint64
    Index uint64
    Type  LogType
    Data  []byte
}

type Log interface {
    FirstIndex() (uint64, error)
    FirstTerm() (uint64, error)
    LastIndex() (uint64, error)
    LastTerm() (uint64, error)
    GetLog(index uint64) (*LogEntry, error)
    StoreLog(log *LogEntry) error
    StoreLogs(logs []*LogEntry) error
    Truncate(index uint64) error
}
