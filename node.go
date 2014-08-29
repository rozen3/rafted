package rafted

type Node interface {
    Client
}

type RaftNode struct {
    backend *HSMBackend
    *RedirectClient
}

func NewRaftNode(backend *HSMBackend, client *RedirectClient) *RaftNode {
    return &RaftNode{
        backend:        backend,
        RedirectClient: client,
    }
}
