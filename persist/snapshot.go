package persist

import "io"

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
    Servers []byte
}

// SnapshotManager is the interface for snapshot maintainment.
// It provides functions to store and restrieve snapshots.
type SnapshotManager interface {
    // Create begins a snapshot at a given index and term,
    // along with the configuration of all servers
    Create(term, index uint64, Servers []byte) (SnapshotWriter, error)

    // List lists metadatas of all durable snapshots.
    // Metadatas shoud be returned in descending order, with the highest index first.
    List() ([]*SnapshotMeta, error)

    // Open take a open operation on snapshot for read. It returns
    // the metadata of the specified snapshot and a ReadCloser for reading it.
    // The ReadCloser must be closed if it is no longer needed.
    Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// SnapshotWriter is the interface to persist the snapshot.
// It's returned by SnapshotManager.Create(). The raft implementation
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
