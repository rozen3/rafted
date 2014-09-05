package rafted

import (
    "errors"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    "time"
)

type LocalState struct {
    *LogStateHead
}

func NewLocalState(super hsm.State, logger logging.Logger) *LocalState {
    object := &LocalState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*LocalState) ID() string {
    return StateLocalID
}

func (self *LocalState) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Entry", self.ID())
    return nil
}

func (self *LocalState) Init(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Init", self.ID())
    sm.QInit(StateFollowerID)
    return nil
}

func (self *LocalState) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Exit", self.ID())
    return nil
}

func (self *LocalState) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch event.Type() {
    case ev.EventQueryStateRequest:
        localHSM, ok := sm.(*LocalHSM)
        hsm.AssertTrue(ok)
        e, ok := event.(*ev.QueryStateRequestEvent)
        hsm.AssertTrue(ok)
        response := &ev.QueryStateResponse{
            StateID: localHSM.StdHSM.State.ID(),
        }
        e.SendResponse(ev.NewQueryStateResponseEvent(response))
        return nil
    case ev.EventPersistError:
        sm.QTranOnEvent(StatePersistErrorID, event)
        return nil
    case ev.EventTerm:
        sm.QTran(hsm.TerminalStateID)
    }
    return self.Super()
}

type NeedPeersState struct {
    *LogStateHead
}

func NewNeedPeersState(super hsm.State, logger logging.Logger) *NeedPeersState {
    object := &NeedPeersState{
        LogStateHead: NewLogStateHead(super, logger),
    }
    super.AddChild(object)
    return object
}

func (*NeedPeersState) ID() string {
    return StateNeedPeersID
}

func (self *NeedPeersState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // coordinate peer
    memberChangeStatus := localHSM.GetMemberChangeStatus()
    switch memberChangeStatus {
    case NewConfigSeen:
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last config")))
            return nil
        }
        localHSM.Peers().AddPeers(GetPeers(localHSM.GetLocalAddr(), conf))
    case NotInMemeberChange:
        fallthrough
    case OldNewConfigSeen:
        fallthrough
    case OldNewConfigCommitted:
        fallthrough
    default:
        conf, err := localHSM.ConfigManager().RNth(0)
        if err != nil {
            localHSM.SelfDispatch(ev.NewPersistErrorEvent(errors.New(
                "fail to read last config")))
        }
        localHSM.Peers().AddPeers(GetPeers(localHSM.GetLocalAddr(), conf))
    }
    localHSM.Peers().Broadcast(ev.NewPeerActivateEvent())
    return nil
}

func (self *NeedPeersState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    // coordinate peer into DeactivatePeerState
    localHSM.Peers().Broadcast(ev.NewPeerDeactivateEvent())
    return nil
}

func (self *NeedPeersState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    return self.Super()
}

type PersistErrorState struct {
    *LogStateHead

    err           error
    notifyTimeout time.Duration
    ticker        Ticker
}

func NewPersistErrorState(
    super hsm.State,
    persistErrorNotifyTimeout time.Duration,
    logger logging.Logger) *PersistErrorState {

    object := &PersistErrorState{
        LogStateHead:  NewLogStateHead(super, logger),
        notifyTimeout: persistErrorNotifyTimeout,
        ticker:        NewSimpleTicker(persistErrorNotifyTimeout),
    }
    super.AddChild(object)
    return object
}

func (*PersistErrorState) ID() string {
    return StatePersistErrorID
}

func (self *PersistErrorState) Entry(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Entry", self.ID())
    localHSM, ok := sm.(*LocalHSM)
    hsm.AssertTrue(ok)
    e, ok := event.(*ev.PersistErrorEvent)
    hsm.AssertTrue(ok)
    self.err = e.Error
    self.Error("%#v", e.Error)
    localHSM.SelfDispatch(ev.NewNotifyPersistErrorEvent(self.err))
    dispatchTimeout := func() {
        localHSM.SelfDispatch(ev.NewNotifyPersistErrorEvent(self.err))
    }
    self.ticker.Start(dispatchTimeout)
    return nil
}

func (self *PersistErrorState) Exit(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Exit", self.ID())
    self.ticker.Stop()
    return nil
}

func (self *PersistErrorState) Handle(
    sm hsm.HSM, event hsm.Event) (state hsm.State) {

    self.Debug("STATE: %s, -> Handle event: %s", self.ID(),
        ev.EventTypeString(event))
    switch {
    case event.Type() == ev.EventPersistError:
        self.Error("already in state: %s, ignore event: %s", self.ID(),
            ev.EventTypeString(event))
        return nil
    case ev.IsClientEvent(event.Type()):
        e, ok := event.(ev.RaftRequestEvent)
        hsm.AssertTrue(ok)
        e.SendResponse(ev.NewPersistErrorResponseEvent(self.err))
        return nil
    }
    return self.Super()
}
