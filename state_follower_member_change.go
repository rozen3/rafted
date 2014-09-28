package rafted

import (
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/zonas/rafted/event"
    logging "github.com/zonas/rafted/logging"
    ps "github.com/zonas/rafted/persist"
)

type FollowerMemberChangeState struct {
    *LogStateHead
}

func NewFollowerMemberChangeState(
    super hsm.State, logger logging.Logger) *FollowerMemberChangeState {

    object := &FollowerMemberChangeState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*FollowerMemberChangeState) ID() string {
    return StateFollowerMemberChangeID
}

func (self *FollowerMemberChangeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *FollowerMemberChangeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *FollowerMemberChangeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    return self.Super()
}

type FollowerOldNewConfigSeenState struct {
    *LogStateHead
}

func NewFollowerOldNewConfigSeenState(
    super hsm.State, logger logging.Logger) *FollowerOldNewConfigSeenState {
    object := &FollowerOldNewConfigSeenState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*FollowerOldNewConfigSeenState) ID() string {
    return StateFollowerOldNewConfigSeenID
}

func (self *FollowerOldNewConfigSeenState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *FollowerOldNewConfigSeenState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *FollowerOldNewConfigSeenState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    switch event.Type() {
    case ev.EventMemberChangeLogEntryCommit:
        e, ok := event.(*ev.MemberChangeLogEntryCommitEvent)
        hsm.AssertTrue(ok)
        localHSM, ok := sm.(*LocalHSM)
        hsm.AssertTrue(ok)
        if !(ps.IsOldNewConfig(e.Message.Conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigSeen) {
            DispatchInconsistantError(localHSM)
            return nil
        }

        localHSM.SetMemberChangeStatus(OldNewConfigCommitted)
        sm.QTran(StateFollowerOldNewConfigCommittedID)
        return nil
    }
    return self.Super()
}

type FollowerOldNewConfigCommittedState struct {
    *LogStateHead
}

func NewFollowerOldNewConfigCommittedState(
    super hsm.State,
    logger logging.Logger) *FollowerOldNewConfigCommittedState {

    object := &FollowerOldNewConfigCommittedState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*FollowerOldNewConfigCommittedState) ID() string {
    return StateFollowerOldNewConfigCommittedID
}

func (self *FollowerOldNewConfigCommittedState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *FollowerOldNewConfigCommittedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *FollowerOldNewConfigCommittedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    switch event.Type() {
    case ev.EventMemberChangeNextStep:
        e, ok := event.(*ev.MemberChangeNextStepEvent)
        hsm.AssertTrue(ok)
        localHSM, ok := sm.(*LocalHSM)
        hsm.AssertTrue(ok)
        conf := e.Message.Conf
        if !(ps.IsNewConfig(conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigCommitted) {
            DispatchInconsistantError(localHSM)
            return nil
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last log index of log")))
            return nil
        }

        nextLogIndex := lastLogIndex + 1
        err = localHSM.ConfigManager().Push(nextLogIndex, conf)
        if err != nil {
            DispatchPushConfigError(localHSM, nextLogIndex)
            return nil
        }
        localHSM.SetMemberChangeStatus(NewConfigSeen)
        sm.QTran(StateFollowerNewConfigSeenID)
        return nil
    }
    return self.Super()
}

type FollowerNewConfigSeenState struct {
    *LogStateHead
}

func NewFollowerNewConfigSeenState(
    super hsm.State, logger logging.Logger) *FollowerNewConfigSeenState {

    object := &FollowerNewConfigSeenState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*FollowerNewConfigSeenState) ID() string {
    return StateFollowerNewConfigSeenID
}

func (self *FollowerNewConfigSeenState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *FollowerNewConfigSeenState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *FollowerNewConfigSeenState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventString(event))
    switch event.Type() {
    case ev.EventMemberChangeLogEntryCommit:
        e, ok := event.(*ev.MemberChangeLogEntryCommitEvent)
        hsm.AssertTrue(ok)
        localHSM, ok := sm.(*LocalHSM)
        hsm.AssertTrue(ok)
        if !(ps.IsNewConfig(e.Message.Conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigCommitted) {
            DispatchInconsistantError(localHSM)
            return nil
        }

        newConf := &ps.Config{
            Servers:    e.Message.Conf.NewServers[:],
            NewServers: nil,
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last log index of log")))
            return nil
        }

        nextLogIndex := lastLogIndex + 1
        err = localHSM.ConfigManager().Push(nextLogIndex, newConf)
        if err != nil {
            DispatchPushConfigError(localHSM, nextLogIndex)
            return nil
        }
        localHSM.SetMemberChangeStatus(NotInMemeberChange)

        if err = localHSM.SendMemberChangeNotify(); err != nil {
            message := fmt.Sprintf(
                "fail to send member change notify, error: %s", err)
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(message)))
        }

        sm.QTran(StateFollowerID)
        return nil
    }
    return self.Super()
}
