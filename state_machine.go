package rafted

import "io"

type StateMachine interface {
    Apply([]byte) interface{}

    MakeSnapshot() (Snapshot, error)
    Restore(io.ReadCloser) error
}

type Snapshot interface {
    Persist(writer SnapshotWriter) error
    Release()
}
