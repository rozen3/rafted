package persist

import "io"

// StateMachine is the interface for implementing state machine in raft.
type StateMachine interface {
    // Apply is invoked once a log entry is commited to state machine.
    Apply([]byte) interface{}

    // MakeSnapshot() is invoked to create a new snapshot for further writing.
    // MakeSnapshot() and Apply() are not called in multiple goroutines,
    // but Apply() could be called concurrently with Snapshot.Persist().
    // Any state machine implementation should ensure the concurrency, which
    // allows concurrent update while a snapshot is persisting.
    MakeSnapshot() (Snapshot, error)

    // Restore() is used to restore the state machine fron a snapshot.
    // It's not called concurrently with any other functions in this interface.
    Restore(io.ReadCloser) error
}

// Snapshot represents a in-memory object to persist snapshot.
// It's returned by StateMachine.MakeSnapshot().
// These functions in this interface should be safe to invoked with concurrent
// call to StateMachine.MakeSnapshot(). Any implementation should ensure that.
type Snapshot interface {
    // Persist is invoked to dump all states to the SnapshotWriter,
    // and close it on completion.
    Persist(writer SnapshotWriter) error

    // Release is invoked when we are finished with this snapshot.
    Release()
}
