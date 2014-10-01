package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
)

type SnapshotInfo struct {
    Leader            *ps.ServerAddress
    Conf              *ps.Config
    LastIncludedTerm  uint64
    LastIncludedIndex uint64
    Size              uint64
    Offset            uint64
}

func write(writer ps.SnapshotWriter, data []byte) error {
    length := len(data)
    for i := 0; i < length; {
        n, err := writer.Write(data[i:])
        if err != nil {
            return err
        }
        i += n
    }
    return nil
}

type SnapshotRecoveryState struct {
    *LogStateHead

    writer ps.SnapshotWriter
    info   *SnapshotInfo
}

func NewSnapshotRecoveryState(
    super hsm.State, logger logging.Logger) *SnapshotRecoveryState {

    object := &SnapshotRecoveryState{
        LogStateHead: NewLogStateHead(super, logger),
        writer:       nil,
        info:         nil,
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

    conf := e.Request.Conf
    lastIncludedTerm := e.Request.LastIncludedTerm
    lastIncludedIndex := e.Request.LastIncludedIndex
    snapshotWriter, err := localHSM.StateMachine().MakeEmptySnapshot(
        lastIncludedTerm, lastIncludedIndex, conf)
    if err != nil {
        self.writer = nil
        self.info = nil
        message := fmt.Sprintf(
            "fail to create snapshot for term: %d, index: %d, error: %s",
            lastIncludedTerm, lastIncludedIndex, err)
        e := errors.New(message)
        localHSM.SelfDispatch(ev.NewAbortSnapshotRecoveryEvent(e))
    } else {
        self.writer = snapshotWriter
        self.info = &SnapshotInfo{
            Leader:            e.Request.Leader,
            Conf:              e.Request.Conf,
            LastIncludedTerm:  e.Request.LastIncludedTerm,
            LastIncludedIndex: e.Request.LastIncludedIndex,
            Size:              e.Request.Size,
            Offset:            uint64(0),
        }
        localHSM.SelfDispatch(event)
    }
    return self.Super()
}

func (self *SnapshotRecoveryState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    // clean up state status
    self.writer = nil
    self.info = nil
    return self.Super()
}

func (self *SnapshotRecoveryState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
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

        if ps.MultiAddrNotEqual(e.Request.Leader, self.info.Leader) {
            // Receive another leader install snapshot request. Just ignore.
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }

        // update last contact time
        followerState, ok := self.Super().(*FollowerState)
        hsm.AssertTrue(ok)
        followerState.UpdateLastContact(localHSM)

        // check the infos consistant
        if ps.ConfigNotEqual(e.Request.Conf, self.info.Conf) ||
            (e.Request.LastIncludedTerm != self.info.LastIncludedTerm) ||
            (e.Request.LastIncludedIndex != self.info.LastIncludedIndex) ||
            (e.Request.Size != self.info.Size) ||
            (e.Request.Offset != self.info.Offset) {
            // Receive request with inconsistant infos to previous ones.
            self.Info("receive inconsistant InstallSnapshotRequest")
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            return nil
        }
        err := write(self.writer, e.Request.Data)
        if err != nil {
            // fail to persist this chunk
            self.Error("fail to persist snapshot chunk, offset: %s, error: %s",
                e.Request.Offset, err)
            if e := self.writer.Cancel(); e != nil {
                self.Error("fail to cancel snapshot writer, error: %s", e)
            }
            e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
            sm.QTran(StateFollowerID)
            return nil
        }
        self.info.Offset += uint64(len(e.Request.Data))

        response.Success = true
        e.SendResponse(ev.NewInstallSnapshotResponseEvent(response))
        // check whether snapshot recovery is done
        if self.info.Offset == self.info.Size {
            snapshotID := self.writer.ID()
            if err := self.writer.Close(); err != nil {
                self.Error("fail to cancel snapshot writer, error: %s", e)
            }
            // Done writing a new snapshot from leader.
            // From now restore log from snapshot.
            if err = localHSM.StateMachine().RestoreFromSnapshot(snapshotID); err != nil {
                // clean up latestly created snapshot
                // NOTICE disable now
                // localHSM.SnapshotManager().Delete(snapshotID)
                message := fmt.Sprintf("fail to restore state machine "+
                    "from snapshot, id: %s, error: %s", snapshotID, err)
                e := errors.New(message)
                localHSM.SelfDispatch(ev.NewPersistErrorEvent(e))
                return nil
            }
            sm.QTran(StateFollowerID)
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
