package rafted

import "io"

type SnapshotMeta struct {
    ID    string
    Term  uint64
    Index uint64
    Size  int64
}

type SnapshotStore interface {
    Create(term, index uint64) (SnapshotWriter, error)
    List() ([]*SnapshotMeta, error)
    Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

type SnapshotWriter interface {
    io.WriteCloser
    ID() string
    Cancel() error
}
