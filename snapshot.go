package rafted

import "io"

type SnapshotMeta struct {
    ID                string
    LastIncludedTerm  uint64
    LastIncludedIndex uint64
    Size              uint64
}

type SnapshotManager interface {
    Create(term, index uint64) (SnapshotWriter, error)
    List() ([]*SnapshotMeta, error)
    Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

type SnapshotWriter interface {
    io.WriteCloser
    ID() string
    Cancel() error
}
