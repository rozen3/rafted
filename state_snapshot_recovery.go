package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
)

type RemoteSnapshot struct {
    Leader            ps.ServerAddr
    Servers           []ps.ServerAddr
    LastIncludedTerm  uint64
    LastIncludedIndex uint64
    Size              uint64
    Offset            uint64
    Chunk             []byte
    writer            ps.SnapshotWriter
}

func NewRemoteSnapshot(
    leader ps.ServerAddr,
    servers []ps.ServerAddr,
    lastIncludedTerm uint64,
    lastIncludedIndex uint64,
    size uint64,
    writer ps.SnapshotWriter) *RemoteSnapshot {
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
        return errors.New("offset mismatch")
    }

    self.Chunk = chunk
    if err := self.Persist(self.writer); err != nil {
        return err
    }
    self.Offset += uint64(len(chunk))
    return nil
}

func (self *RemoteSnapshot) Persist(writer ps.SnapshotWriter) error {
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
    *LogStateHead

    writer   ps.SnapshotWriter
    snapshot *RemoteSnapshot
}

func NewSnapshotRecoveryState(
    super hsm.State, logger logging.Logger) *SnapshotRecoveryState {

    object := &SnapshotRecoveryState{
        LogStateHead: NewLogStateHead(super, logger),
        writer:       nil,
        snapshot:     nil,
    }
    super.AddChild(object)
    return object
}

func (*SnapshotRecoveryState) ID() string {
    return StateSnapshotRecoveryID
}

func (self *SnapshotRecoveryState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    hsm.AssertEqual(event.Type(), ev.EventInstallSnapshotRequest)
    e, ok := event.(*ev.InstallSnapshotRequestEvent)
    hsm.AssertTrue(ok)

    servers := e.Request.Servers
    lastIncludedTerm := e.Request.LastIncludedTerm
    lastIncludedIndex := e.Request.LastIncludedIndex
    snapshotWriter, err := localHSM.SnapshotManager().Create(
        lastIncludedTerm, lastIncludedIndex, servers)
    if err != nil {
        self.writer = nil
        self.snapshot = nil
        message := fmt.Sprintf(
            "fail to create snapshot for term: %d, index: %d, error: %s",
            lastIncludedTerm, lastIncludedIndex, err)
        e := errors.New(message)
        localHSM.SelfDispatch(ev.NewAbortSnapshotRecoveryEvent(e))
    } else {
        self.writer = snapshotWriter
        self.snapshot = NewRemoteSnapshot(
            e.Request.Leader,
            e.Request.Servers,
            e.Request.LastIncludedTerm,
            e.Request.LastIncludedIndex,
            e.Request.Size,
            self.writer)
        localHSM.SelfDispatch(event)
    }
    return self.Super()
}

func (self *SnapshotRecoveryState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // clean up state status
    self.writer = nil
    self.snapshot = nil
    return self.Super()
}

func (self *SnapshotRecoveryState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventInstallSnapshotRequest:
        e, ok := event.(*ev.InstallSnapshotRequestEvent)
        hsm.AssertTrue(ok)
        term := localHSM.GetCurrentTerm()
        response := &ev.InstallSnapshotResponse{
            Term:    term,
            Success: false,
        }
        if e.Request.Term < term {
            // ignore stale request
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }

        if e.Request.Term > term {
            localHSM.SetCurrentTermWithNotify(e.Request.Term)
            localHSM.SetLeaderWithNotify(e.Request.Leader)
            localHSM.SelfDispatch(event)
            localHSM.QTran(StateFollowerID)
            return nil
        }

        if ps.AddrNotEqual(&e.Request.Leader, &self.snapshot.Leader) {
            // Receive another leader install snapshot request. Just ignore.
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }

        // update last contact time
        followerState, ok := self.Super().(*FollowerState)
        hsm.AssertTrue(ok)
        followerState.UpdateLastContact(localHSM)

        // check the infos consistant
        if ps.AddrsNotEqual(e.Request.Servers, self.snapshot.Servers) ||
            (e.Request.LastIncludedTerm != self.snapshot.LastIncludedTerm) ||
            (e.Request.LastIncludedIndex != self.snapshot.LastIncludedIndex) ||
            (e.Request.Size != self.snapshot.Size) ||
            (e.Request.Offset != self.snapshot.Offset) {
            // Receive request with inconsistant infos to previous ones.
            self.Info("receive inconsistant InstallSnapshotRequest")
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }
        err := self.snapshot.PersistChunk(e.Request.Offset, e.Request.Data)
        if err != nil {
            // fail to persist this chunk
            self.Error("fail to persist snapshot chunk, offset: %s, error: %s",
                e.Request.Offset, err)
            if e := self.writer.Cancel(); e != nil {
                self.Error("fail to cancel snapshot writer, error: %s", e)
            }
            self.snapshot.Release()
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            sm.QTran(StateFollowerID)
            return nil
        }

        response.Success = true
        e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
        // check whether snapshot recovery is done
        if self.snapshot.Offset == self.snapshot.Size {
            snapshotID := self.writer.ID()
            if err := self.writer.Close(); err != nil {
                self.Error("fail to cancel snapshot writer, error: %s", e)
            }
            self.snapshot.Release()
            // Done writing a new snapshot from leader.
            // From now restore log from snapshot.
            _, reader, err := localHSM.SnapshotManager().Open(snapshotID)
            if err != nil {
                message := fmt.Sprintf(
                    "fail to open snapshot, id: %s, error: %s", snapshotID, err)
                e := errors.New(message)
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
                return nil
            }
            success := true
            if err = localHSM.StateMachine().Restore(reader); err != nil {

                // clean up latestly created snapshot
                // NOTICE disable now
                // localHSM.SnapshotManager().Delete(snapshotID)
                message := fmt.Sprintf("fail to restore state machine "+
                    "from snapshot, id: %s, error: %s", snapshotID, err)
                e := errors.New(message)
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
                success = false
            }
            if err = reader.Close(); err != nil {
                self.Error("fail to close reader of snapshot id: %s",
                    snapshotID)
            }
            if success {
                sm.QTran(StateFollowerID)
            }
        }
        return nil
    case ev.EventTimeoutElection:
        // TODO add a breakout policy for this state
        // Ignore this event. Don't transfer to candidate state when
        // recovering from snapshot.
        return nil
    case ev.EventAbortSnapshotRecovery:
        e, ok := event.(*ev.AbortSnapshotRecoveryEvent)
        hsm.AssertTrue(ok)
        localHSM.SelfDispatch(ev.NewPersistErrorEvent(e.Error))
        sm.QTran(StateFollowerID)
        return nil
    }
    return self.Super()
}
