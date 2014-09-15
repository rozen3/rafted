package persist

import (
    "errors"
    "io"
)

var (
    ErrorSnapshotNotFound error = errors.New("Snapshot not found")
    ErrorNoSnapshot             = errors.New("No snapshot")
)

// Metadata for a snapshot.
type SnapshotMeta struct {
    // unique id for every snapshot, opaquely used in SnapshotManager.
    ID string
    // the snapshot replaces all entries up through and including this index
    LastIncludedTerm uint64
    // term of lastIncludedIndex
    LastIncludedIndex uint64
    // total size of this snapshot
    Size uint64
    // the configuration of all servers
    Conf *Config
}

// StateMachine is the interface for implementing state machine and
// snapshot management in raft.
type StateMachine interface {
    // Apply is invoked once a log entry is commited to state machine.
    Apply([]byte) []byte

    // MakeSnapshot() is invoked to create a local snapshot.
    // MakeSnapshot() and Apply() could be called in multiple goroutines.
    // Any state machine implementation should ensure the concurrency, which
    // allows concurrent update while a snapshot is persisting.
    // The arguments are the last term included in this snapshot,
    // the last index included and the corresponding servers configuration.
    MakeSnapshot(
        lastIncludedTerm uint64,
        lastIncludeIndex uint64,
        conf *Config) (id string, err error)

    // MakeEmptySnapshot() in invoked to create a empty snapshot for receiving
    // snapshot from raft leader in the procedure of snapshot recoverage.
    // MakeEmptySnapshot() and Apply() are not called in multiple goroutines.
    // the arguments are the same with MakeSnapshot().
    MakeEmptySnapshot(
        lastIncludeTerm uint64,
        lastIncludeIndex uint64,
        conf *Config) (SnapshotWriter, error)

    // RestoreFromSnapshot() is used to restore the state machine from
    // the snapshot of specified id.
    // It's not called concurrently with Apply().
    // Return ErrorSnapshotNotFound if no snapshot with specified id.
    RestoreFromSnapshot(id string) error

    // LastSnapshotInfo() returns the info of the latest snapshot.
    // Return ErrorNoSnapshot if no snapshot at all.
    LastSnapshotInfo() (*SnapshotMeta, error)

    // AllSnapshotInfo() lists metadatas of all durable snapshots(
    // not including the unfinished snapshot). Metadatas shoud be returned
    // in descending order, with the highest index first.
    // Return ErrorNoSnapshot if there is not snapshot at all.
    AllSnapshotInfo() ([]*SnapshotMeta, error)

    // OpenSnapshot() open a snapshot for read. It returns the metadata
    // of the specified snapshot and a ReadCloser for reading it.
    // The ReadCloser must be closed if it is no longer needed.
    // Return ErrorSnapshotNotFound if no snapshot with specified id.
    OpenSnapshot(id string) (*SnapshotMeta, io.ReadCloser, error)

    // Delete removes a snapshot.
    // Return ErrorSnapshotNotFound if no snapshot with specified id.
    DeleteSnapshot(id string) error
}

// SnapshotWriter is the interface to persist snapshot.
// It's returned by StateMachine.MakeEmptySnapshot(). The raft implementation
// would write snapshot into the stream and close it on completion.
// Cancel will be called on error.
type SnapshotWriter interface {
    // Stream writer for snapshot persistance
    io.WriteCloser

    // Returns ID of this corresponding snapshot
    ID() string

    // Cancels the snapshot writing
    Cancel() error
}
