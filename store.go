package rafted

import "io"

type DataStore interface {
    Apply([]byte) interface{}

    //    MakeSnapshot() (Snapshot, error)
    //    Restore(io.ReadCloser) error
}

type Snapshot interface {
    Make()
    write()
    Persist(writer SnapshotWriter) error
    Release()
}
