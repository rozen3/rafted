package rafted

import (
    "bytes"
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    persist "github.com/hhkbp2/rafted/persist"
)

type RemoteSnapshot struct {
    Leader            []byte
    Servers           []byte
    LastIncludedTerm  uint64
    LastIncludedIndex uint64
    Size              uint64
    Offset            uint64
    Chunk             []byte
    writer            persist.SnapshotWriter
}

func NewRemoteSnapshot(
    leader []byte,
    servers []byte,
    lastIncludedTerm uint64,
    lastIncludedIndex uint64,
    size uint64,
    writer persist.SnapshotWriter) *RemoteSnapshot {
    return &RemoteSnapshot{
        Leader:            leader,
        Servers:           servers,
        LastIncludedTerm:  lastIncludedTerm,
        LastIncludedIndex: lastIncludedIndex,
        Size:              size,
        Offset:            0,
        Chunk:             make([]byte, 0),
        writer:            writer,
    }
}

func (self *RemoteSnapshot) PersistChunk(offset uint64, chunk []byte) error {
    if offset != self.Offset {
        // TODO add log
        return errors.New("offset mismatch")
    }

    self.Chunk = chunk
    if err := self.Persist(self.writer); err != nil {
        return err
    }
    self.Offset += uint64(len(chunk))
    return nil
}

func (self *RemoteSnapshot) Persist(writer persist.SnapshotWriter) error {
    length := len(self.Chunk)
    for i := 0; i < length; {
        n, err := writer.Write(self.Chunk[i:])
        if err != nil {
            return err
        }
        i += n
    }
    return nil
}

func (self *RemoteSnapshot) Release() {
}

type SnapshotRecoveryState struct {
    *hsm.StateHead

    writer   persist.SnapshotWriter
    snapshot *RemoteSnapshot
}

func NewSnapshotRecoveryState(super hsm.State) *SnapshotRecoveryState {
    object := &SnapshotRecoveryState{
        StateHead: hsm.NewStateHead(super),
        writer:    nil,
        snapshot:  nil,
    }
    super.AddChild(object)
    return object
}

func (*SnapshotRecoveryState) ID() string {
    return StateSnapshotRecoveryID
}

func (self *SnapshotRecoveryState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Entry")

    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    hsm.AssertEqual(event.Type(), ev.EventInstallSnapshotRequest)
    e, ok := event.(*ev.InstallSnapshotRequestEvent)
    hsm.AssertTrue(ok)

    servers := e.Request.Servers
    lastIncludedTerm := e.Request.LastIncludedTerm
    lastIncludedIndex := e.Request.LastIncludedIndex
    snapshotWriter, err := raftHSM.SnapshotManager.Create(
        lastIncludedTerm, lastIncludedIndex, servers)
    if err != nil {
        // TODO add log
        self.writer = nil
    } else {
        self.writer = snapshotWriter
        self.snapshot = NewRemoteSnapshot(
            e.Request.Leader,
            e.Request.Servers,
            e.Request.LastIncludedTerm,
            e.Request.LastIncludedIndex,
            e.Request.Size,
            self.writer)
    }

    // TODO add some policy to jump out this state if it fails to get it done.

    return self.Super()
}

func (self *SnapshotRecoveryState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Exit")
    // clean up state status
    self.writer = nil
    self.snapshot = nil
    return self.Super()
}

func (self *SnapshotRecoveryState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    fmt.Println(self.ID(), "-> Handle, event=", event)
    raftHSM, ok := sm.(*RaftHSM)
    hsm.AssertTrue(ok)
    switch {
    case event.Type() == ev.EventTimeoutHeartBeat:
        // Ignore this event. Don't transfer to candidate state when
        // recovering from snapshot.
        return nil
    case event.Type() == ev.EventInstallSnapshotRequest:
        if self.writer == nil {
            // fail to create snapshot writer on entry handler
            // TODO add log
            if err := self.writer.Cancel(); err != nil {
                // TODO add log
            }
            sm.QTran(StateFollowerID)
            return nil
        }
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        if bytes.Compare(e.Request.Leader, self.snapshot.Leader) != 0 {
            // Receive another leader install snapshot request.
            // This snapshot recovery procedure is interrupted.
            // TODO log
            if err := self.writer.Cancel(); err != nil {
                // TODO add log
            }
            sm.QTran(StateFollowerID)
            return nil
        }
        if bytes.Compare(e.Request.Servers, self.snapshot.Servers) != 0 {
            // Receive inconsistant server configurations among
            // install snapshot requests.
            if err := self.writer.Cancel(); err != nil {
                // TODO add log
            }
            sm.QTran(StateFollowerID)
            return nil
        }
        if e.Request.Term < raftHSM.GetCurrentTerm() {
            // The request is stale with a outdated term.
            if err := self.writer.Cancel(); err != nil {
                // TODO add log
            }
            sm.QTran(StateFollowerID)
            return nil
        }
        if (e.Request.LastIncludedTerm != self.snapshot.LastIncludedTerm) ||
            (e.Request.LastIncludedIndex != self.snapshot.LastIncludedIndex) ||
            (e.Request.Size != self.snapshot.Size) ||
            (e.Request.Offset != self.snapshot.Offset) {
            // Receive invalid request for previously transfered snapshot,
            // just ignore them.
            return nil
        }
        response := &ev.InstallSnapshotResponse{
            Term:    raftHSM.GetCurrentTerm(),
            Success: false,
        }
        err := self.snapshot.PersistChunk(e.Request.Offset, e.Request.Data)
        if err != nil {
            // fail to persist this chunk
            if e := self.writer.Cancel(); e != nil {
                // TODO add log
            }
            self.snapshot.Release()
            // TODO error handling, add log
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            sm.QTran(StateFollowerID)
        } else {
            response.Success = true
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            // check whether snapshot recovery is done
            if self.snapshot.Offset == self.snapshot.Size {
                if err := self.writer.Close(); err != nil {
                    // TODO add log
                }
                self.snapshot.Release()
                sm.QTran(StateFollowerID)
            }
        }
        return nil
    }
    return self.Super()
}