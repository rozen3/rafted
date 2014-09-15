package rafted

import (
    ev "github.com/hhkbp2/rafted/event"
)

type Node interface {
    Client
    Notifiable
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

func (self *RaftNode) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.backend.GetNotifyChan()
}
