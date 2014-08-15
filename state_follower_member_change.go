package rafted

import (
    logging "github.com/hhkbp2/rafted/logging"
)

type FollowerMemberChangeState struct {
    *LogStateHead
}

func NewFollowerMemberChangeState(
    super hsm.State, logger logging.Logger) *FollowerMemberChangeState {

    object := &FollowerState{
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
        ev.PrintEvent(event))
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
        ev.PrintEvent(event))
    switch event.Type() {
    case ev.EventMemberLogEntryCommit:
        e, ok := event.(*MemberChangeLogEntryCommitEvent)
        hsm.AssertTrue(ok)
        if !(IsOldNewConfig(e.Message.Conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigSeen) {
            // TODO error handling
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
        ev.PrintEvent(event))
    switch event.Type() {
    case ev.EventMemberChangeNextStep:
        e, ok := event.(*MemberChangeNextStepEvent)
        hsm.AssertTrue(ok)
        if !(IsNewConfig(e.Message.Conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigCommitted) {
            // TODO error handling
        }

        if err := localHSM.ConfigManager().PushConfig(conf); err != nil {
            // TODO error handling
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
        ev.PrintEvent(event))
    switch event.Type() {
    case ev.EventMemberLogEntryCommit:
        e, ok := event.(*MemberChangeLogEntryCommitEvent)
        hsm.AssertTrue(ok)
        if !(IsNewConfig(e.Message.Conf) &&
            localHSM.GetMemberChangeStatus() == OldNewConfigCommitted) {
            // TODO error handling
        }

        newConf := &persist.Config{
            Servers:    e.Message.Conf.NewServers[:],
            NewServers: nil,
        }
        if err = configManager.PushConfig(newConf); err != nil {
            // TODO error handling
        }
        localHSM.SetMemberChangeStatus(NotInMemeberChange)
        sm.QTran(StateFollowerID)
        return nil
    }
    return self.Super()
}
