package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
)

type LeaderMemberChangeHSM struct {
    *hsm.StdHSM

    LeaderState *LeaderState
    LocalHSM    *LocalHSM
}

func NewLeaderMemberChangeHSM(
    top hsm.State, initial hsm.State) *LeaderMemberChangeHSM {

    return &LeaderMemberChangeHSM{
        StdHSM: hsm.NewStdHSM(HSMTypeLeaderMemberChange, top, initial),
    }
}

func (self *LeaderMemberChangeHSM) Init() {
    self.StdHSM.Init2(self, hsm.NewStdEvent(hsm.EventInit))
}

func (self *LeaderMemberChangeHSM) Dispatch(event hsm.Event) {
    self.StdHSM.Dispatch2(self, event)
}

func (self *LeaderMemberChangeHSM) QTran(targetStateID string) {
    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSM(self, target)
}

func (self *LeaderMemberChangeHSM) QTranOnEvent(
    targetStateID string, event hsm.Event) {

    target := self.StdHSM.LookupState(targetStateID)
    self.StdHSM.QTranHSMOnEvents(self, target, event, event, event)
}

func (self *LeaderMemberChangeHSM) SetLeaderState(leaderState *LeaderState) {
    self.LeaderState = leaderState
}

func (self *LeaderMemberChangeHSM) SetLocalHSM(localHSM *LocalHSM) {
    self.LocalHSM = localHSM
}

type LeaderMemberChangeState struct {
    *LogStateHead
}

func NewLeaderMemberChangeState(
    super hsm.State, logger logging.Logger) *LeaderMemberChangeState {

    object := &LeaderMemberChangeState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderMemberChangeState) ID() string {
    return StateLeaderMemberChangeID
}

func (self *LeaderMemberChangeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderMemberChangeState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    sm.QInit(StateLeaderMemberChangeDeactivatedID)
    return nil
}

func (self *LeaderMemberChangeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    return self.Super()
}

type LeaderMemberChangeDeactivatedState struct {
    *LogStateHead
}

func NewLeaderMemberChangeDeactivatedState(
    super hsm.State, logger logging.Logger) *LeaderMemberChangeDeactivatedState {

    object := &LeaderMemberChangeDeactivatedState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderMemberChangeDeactivatedState) ID() string {
    return StateLeaderMemberChangeDeactivatedID
}

func (self *LeaderMemberChangeDeactivatedState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderMemberChangeDeactivatedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangeDeactivatedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch event.Type() {
    case ev.EventLeaderMemberChangeActivate:
        sm.QTran(StateLeaderMemberChangeActivatedID)
        return nil
    }
    return self.Super()
}

type LeaderMemberChangeActivatedState struct {
    *LogStateHead
}

func NewLeaderMemberChangeActivatedState(
    super hsm.State, logger logging.Logger) *LeaderMemberChangeActivatedState {

    object := &LeaderMemberChangeActivatedState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderMemberChangeActivatedState) ID() string {
    return StateLeaderMemberChangeActivatedID
}

func (self *LeaderMemberChangeActivatedState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderMemberChangeActivatedState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    switch memberChangeHSM.LocalHSM.GetMemberChangeStatus() {
    case OldNewConfigSeen:
    case OldNewConfigCommitted:
    case NewConfigSeen:
        sm.QInit(StateLeaderInMemberChangeID)
    case NotInMemeberChange:
        fallthrough
    default:
        sm.QInit(StateLeaderNotInMemberChangeID)
    }
    return nil
}

func (self *LeaderMemberChangeActivatedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangeActivatedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch event.Type() {
    case ev.EventLeaderMemberChangeDeactivate:
        sm.QTran(StateLeaderMemberChangeDeactivatedID)
        return nil
    }
    return self.Super()
}

type LeaderNotInMemberChangeState struct {
    *LogStateHead
}

func NewLeaderNotInMemberChangeState(
    super hsm.State, logger logging.Logger) *LeaderNotInMemberChangeState {

    object := &LeaderNotInMemberChangeState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderNotInMemberChangeState) ID() string {
    return StateLeaderNotInMemberChangeID
}

func (self *LeaderNotInMemberChangeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    localHSM := memberChangeHSM.LocalHSM
    localHSM.SetMemberChangeStatus(NotInMemeberChange)
    return nil
}

func (self *LeaderNotInMemberChangeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderNotInMemberChangeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    localHSM := memberChangeHSM.LocalHSM
    leaderState := memberChangeHSM.LeaderState
    switch event.Type() {
    case ev.EventClientMemberChangeRequest:
        e, ok := event.(*ev.ClientMemberChangeRequestEvent)
        hsm.AssertTrue(ok)
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            // TODO error handling
        }
        if !(ps.IsNormalConfig(conf) &&
            ps.AddrsEqual(conf.Servers, e.Request.OldServers)) {

            // TODO error handling
        }

        newConf := &ps.Config{
            Servers:    e.Request.OldServers[:],
            NewServers: e.Request.NewServers[:],
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }
        err = localHSM.ConfigManager().Push(lastLogIndex+1, newConf)
        if err != nil {
            // TODO error handling
        }

        request := &InflightRequest{
            LogType:    ps.LogMemberChange,
            Conf:       newConf,
            ResultChan: e.ClientRequestEventHead.ResultChan,
        }
        if err := leaderState.StartFlight(localHSM, request); err != nil {
            // TODO error handling
        }

        localHSM.SetMemberChangeStatus(OldNewConfigSeen)
        sm.QTran(StateLeaderMemberChangePhase1ID)
        return nil
    }
    return self.Super()
}

type LeaderInMemberChangeState struct {
    *LogStateHead
}

func NewLeaderInMemberChangeState(
    super hsm.State, logger logging.Logger) *LeaderInMemberChangeState {

    object := &LeaderInMemberChangeState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderInMemberChangeState) ID() string {
    return StateLeaderInMemberChangeID
}

func (self *LeaderInMemberChangeState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderInMemberChangeState) Init(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Init", self.ID())
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    localHSM := memberChangeHSM.LocalHSM
    switch memberChangeHSM.LocalHSM.GetMemberChangeStatus() {
    case OldNewConfigSeen:
        localHSM.SelfDispatch(ev.NewLeaderReenterMemberChangeStateEvent())
        sm.QInit(StateLeaderMemberChangePhase1ID)
    case OldNewConfigCommitted:
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            // TODO error handling
        }
        message := &ev.LeaderForwardMemberChangePhase{
            Conf: conf,
        }
        localHSM.SelfDispatch(
            ev.NewLeaderForwardMemberChangePhaseEvent(message))
        sm.QInit(StateLeaderMemberChangePhase1ID)
    case NewConfigSeen:
        localHSM.SelfDispatch(ev.NewLeaderReenterMemberChangeStateEvent())
        sm.QInit(StateLeaderMemberChangePhase2ID)
    }
    return nil
}

func (self *LeaderInMemberChangeState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderInMemberChangeState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch event.Type() {
    case ev.EventClientMemberChangeRequest:
        e, ok := event.(*ev.ClientMemberChangeRequestEvent)
        hsm.AssertTrue(ok)
        e.SendResponse(ev.NewLeaderInMemberChangeResponseEvent())
        return nil
    }

    return self.Super()
}

type LeaderMemberChangePhase1State struct {
    *LogStateHead
}

func NewLeaderMemberChangePhase1State(
    super hsm.State, logger logging.Logger) *LeaderMemberChangePhase1State {

    object := &LeaderMemberChangePhase1State{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderMemberChangePhase1State) ID() string {
    return StateLeaderMemberChangePhase1ID
}

func (self *LeaderMemberChangePhase1State) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderMemberChangePhase1State) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangePhase1State) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    localHSM := memberChangeHSM.LocalHSM
    leaderState := memberChangeHSM.LeaderState
    switch event.Type() {
    case ev.EventLeaderReenterMemberChangeState:
        // Re-replicate the logs update util now, which include
        // these member change ones.
        // Since peers would automatically start replicating when
        // it enters leader peer state, do nothing here.
        return nil
    case ev.EventLeaderForwardMemberChangePhase:
        e, ok := event.(*ev.LeaderForwardMemberChangePhaseEvent)
        hsm.AssertTrue(ok)
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            // TODO error handling
        }
        if !(ps.IsOldNewConfig(conf)) && ps.ConfigEqual(e.Message.Conf, conf) {
            // TODO error handling
        }

        // update member change status
        localHSM.SetMemberChangeStatus(OldNewConfigCommitted)

        newConf := &ps.Config{
            Servers:    nil,
            NewServers: e.Message.Conf.NewServers[:],
        }
        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }

        err = localHSM.ConfigManager().Push(lastLogIndex+1, newConf)
        if err != nil {
            // TODO error handling
        }
        request := &InflightRequest{
            LogType:    ps.LogMemberChange,
            Conf:       newConf,
            ResultChan: e.Message.ResultChan,
        }
        if err := leaderState.StartFlight(localHSM, request); err != nil {
            // TODO error handling
        }

        // update member change status
        localHSM.SetMemberChangeStatus(OldNewConfigCommitted)

        sm.QTran(StateLeaderMemberChangePhase2ID)
        return nil
    }
    return self.Super()
}

type LeaderMemberChangePhase2State struct {
    *LogStateHead
}

func NewLeaderMemberChangePhase2State(
    super hsm.State, logger logging.Logger) *LeaderMemberChangePhase2State {

    object := &LeaderMemberChangePhase2State{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LeaderMemberChangePhase2State) ID() string {
    return StateLeaderMemberChangePhase2ID
}

func (self *LeaderMemberChangePhase2State) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LeaderMemberChangePhase2State) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangePhase2State) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    memberChangeHSM, ok := sm.(*LeaderMemberChangeHSM)
    hsm.AssertTrue(ok)
    localHSM := memberChangeHSM.LocalHSM
    switch event.Type() {
    case ev.EventLeaderReenterMemberChangeState:
        // Re-replicate the logs update util now, which include
        // these member change ones.
        // Since peers would automatically start replicating when
        // it enters leader peer state, do nothing here.
        return nil
    case ev.EventLeaderForwardMemberChangePhase:
        e, ok := event.(*ev.LeaderForwardMemberChangePhaseEvent)
        hsm.AssertTrue(ok)
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            // TODO error handling
        }
        if !(ps.IsNewConfig(conf) && ps.ConfigEqual(e.Message.Conf, conf)) {
            // TODO error handling
        }

        newConf := &ps.Config{
            Servers:    e.Message.Conf.NewServers[:],
            NewServers: nil,
        }

        lastLogIndex, err := localHSM.Log().LastIndex()
        if err != nil {
            // TODO error handling
        }

        err = localHSM.ConfigManager().Push(lastLogIndex+1, newConf)
        if err != nil {
            // TODO error handling
        }

        // update member change status
        localHSM.SetMemberChangeStatus(NewConfigCommitted)

        // response client
        response := &ev.ClientResponse{
            Success: true,
        }
        if e.Message.ResultChan != nil {
            e.Message.ResultChan <- ev.NewClientResponseEvent(response)
        }

        // TODO stepdown if we are not part of the new cluster

        if err = localHSM.SendMemberChangeNotify(); err != nil {
            // TODO error handling
        }

        sm.QTran(StateLeaderNotInMemberChangeID)
        return nil
    }
    return self.Super()
}

func SetupLeaderMemberChangeHSM(logger logging.Logger) *LeaderMemberChangeHSM {
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateLeaderMemberChangeID)
    leaderMemberChangeState := NewLeaderMemberChangeState(top, logger)
    NewLeaderMemberChangeDeactivatedState(leaderMemberChangeState, logger)
    activatedState := NewLeaderMemberChangeActivatedState(
        leaderMemberChangeState, logger)
    NewLeaderNotInMemberChangeState(activatedState, logger)
    inMemberChangeState := NewLeaderInMemberChangeState(activatedState, logger)
    NewLeaderMemberChangePhase1State(inMemberChangeState, logger)
    NewLeaderMemberChangePhase2State(inMemberChangeState, logger)
    leaderMemberChangeHSM := NewLeaderMemberChangeHSM(top, initial)
    return leaderMemberChangeHSM
}
