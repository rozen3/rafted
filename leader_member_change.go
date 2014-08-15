package rafted

import (
    hsm "github.com/hhkbp2/go-hsm"
    logging "github.com/hhkbp2/rafted/logging"
    "sync"
)

type LeaderMemberChangeHSM struct {
    *hsm.StdHSM
}

func NewLeaderMemberChangeHSM(
    top hsm.State, initial hsm.State) *LeaderMemberChangeHSM {

    return &LeaderMemberChangeHSM{
        StdHSM: hsm.NewStdHSM(HSMType),
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
    switch localHSM.GetMemberChangeStatus() {
    case NotInMemeberChange:
        // must transfer from sync state, update member change status
        localHSM.SetMemberChangeStatus(OldNewConfigSeen)
        sm.QInit(StateLeaderMemberChangePhase1ID)
    case OldNewConfigSeen:
        localHSM.SelfDispatch(ev.NewReenterMemberChangeStateEvent())
        sm.QInit(StateLeaderMemberChangePhase1ID)
    case OldNewConfigCommitted:
        localHSM.SelfDispatch(ev.NewForwardMemberChangePhaseEvent())
        sm.QInit(StateLeaderMemberChangePhase1ID)
    case NewConfigSeen:
        localHSM.SelfDispatch(ev.NewReenterMemberChangeStateEvent())
        sm.QInit(StateLeaderMemberChangePhase2ID)
    }
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
        ev.PrintEvent(event))
    switch event.Type() {
    case ev.EventClientMemberChangeRequest:
        e, ok := event.(*ClientMemberChangeRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.LeaderInMemberChangeResponse{}
        e.Response(ev.NewLeaderInMemberChangeResponseEvent(response))
        return nil
    }
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

func (self *LedaerMemberChangeDeactivatedState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LeaderMemberChangeDeactivatedState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.PrintEvent(event))
    switch event.Type() {
    case ev.EventLeaderMemberChangeActivate:
        sm.QTran(StateLeaderMemberChangeNotIn)
        return nil
    }
    return self.Super()
}

type LeaderNotInMemberChangeState struct {
    *LogStateHead
}

func NewLeaderNotInMemberChangeState(
    super hsm.State, logger logging.Logger) *LeaderNotInMemberChangeState {

    // TODO
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
        ev.PrintEvent(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventReenterMemberChangeState:
        // scan the log, find member change entry and replicate it
        committedIndex, err := localHSM.Log().CommittedIndex()
        if err != nil {
            // TODO error handling
        }

        // TODO add impl
        return nil
    case ev.EventForwardMemberChangePhase:
        e, ok := event.(*ForwardMemberChangePhaseEvent)
        hsm.AssertTrue(ok)
        configManager := local.ConfigManager()
        conf, err := configManager.LastConfig()
        if err != nil {
            // TODO error handling
        }
        if !(IsOldNewConfig(conf)) &&
            persist.ConfigEqual(e.Message.Conf, conf) {

            // TODO error handling
        }

        // update member change status
        localHSM.SetMemberChangeStatus(OldNewConfigCommitted)

        newConf := &persist.Config{
            Servers:    nil,
            NewServers: e.Message.Conf.NewServers[:],
        }
        if err = localHSM.ConfigManager().PushConfig(newConf); err != nil {
            // TODO error handling
        }
        request := &InflightRequest{
            LogType:    persist.LogMemberChange,
            Conf:       newConf,
            ResultChan: e.Message.ResultChan,
        }
        leaderState, ok := self.Super().Super().(*LeaderState)
        hsm.AssertTrue(ok)
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
        ev.PrintEvent(event))
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    switch event.Type() {
    case ev.EventReenterMemberChangeState:
        // TODO add impl
    case ev.EventForwardMemberChangePhase:
        // TODO add impl
        e, ok := event.(*ForwardMemberChangePhaseEvent)
        hsm.AssertTrue(ok)
        configManager := local.ConfigManager()
        conf, err := configManager.LastConfig()
        if err != nil {
            // TODO error handling
        }
        if !(IsNewConfig(conf) &&
            persist.ConfigEqual(e.Message.Conf, conf)) {

            // TODO error handling
        }

        // update member change status
        localHSM.SetMemberChangeStatus(NewConfigCommitted)

        // response client
        response := &ev.ClientResponse{
            Success: true,
        }
        e.Message.ResultChan <- response

        // TODO stepdown if we are not part of the new cluster

        sm.QTran(StateSyncID)
        return nil
    }
    return self.Super()
}

type LeaderMemberChange struct {
    *LeaderMemberChangeHSM
}

func NewLeaderMemberChange() *LeaderMemberChange {
    // TODO add impl
    top := hsm.NewTop()
    initial := hsm.NewInitial(top, StateLeaderMemberChangeID)
    leaderMemberChangeState := NewLeaderMemberChangeState(
        top, StateLeaderMemberChangeID)
    // TODO
}
