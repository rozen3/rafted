package rafted

import "io"

type DataStore interface {
    Apply(*Log) interface{}

    MakeSnapshot() (Snapshot, error)
    Restore(io.ReadCloser) error
}

type Snapshot interface {
    Persist(writer SnapshotWriter) error
    Release()
}
