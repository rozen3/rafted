package rafted

import (
    "container/list"
    "errors"
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ev "github.com/hhkbp2/rafted/event"
    logging "github.com/hhkbp2/rafted/logging"
    ps "github.com/hhkbp2/rafted/persist"
    "io"
    "sync"
)

// The general interface of event channel.
type EventChannel interface {
    Send(hsm.Event)
    Recv() hsm.Event
    Close()
}

// ReliableEventChannel is an unlimited size channel for
// non-blocking event sending/receiving.
type ReliableEventChannel struct {
    inChan    chan hsm.Event
    outChan   chan hsm.Event
    closeChan chan interface{}
    queue     *list.List
    group     *sync.WaitGroup
}

func NewReliableEventChannel() *ReliableEventChannel {
    object := &ReliableEventChannel{
        // all underlaid channals should not buffer.
        inChan:    make(chan hsm.Event, 0),
        outChan:   make(chan hsm.Event, 0),
        closeChan: make(chan interface{}, 0),
        queue:     list.New(),
        group:     &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *ReliableEventChannel) Start() {
    routine := func() {
        defer self.group.Done()
        for {
            if self.queue.Len() > 0 {
                e := self.queue.Front()
                outEvent, _ := e.Value.(hsm.Event)
                select {
                case <-self.closeChan:
                    return
                case inEvent := <-self.inChan:
                    self.queue.PushBack(inEvent)
                case self.outChan <- outEvent:
                    self.queue.Remove(e)
                }
            } else {
                select {
                case <-self.closeChan:
                    return
                case event := <-self.inChan:
                    self.queue.PushBack(event)
                }
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *ReliableEventChannel) Send(event hsm.Event) {
    self.inChan <- event
}

func (self *ReliableEventChannel) Recv() hsm.Event {
    event := <-self.outChan
    return event
}

func (self *ReliableEventChannel) GetInChan() chan<- hsm.Event {
    return self.inChan
}

func (self *ReliableEventChannel) GetOutChan() <-chan hsm.Event {
    return self.outChan
}

func (self *ReliableEventChannel) Close() {
    self.closeChan <- self
    self.group.Wait()
}

// ReliableUint64Channel is an unlimited size channel for
// non-blocking sending/receiving
type ReliableUint64Channel struct {
    inChan    chan uint64
    outChan   chan uint64
    closeChan chan interface{}
    queue     *list.List
    group     *sync.WaitGroup
}

func NewReliableUint64Channel() *ReliableUint64Channel {
    object := &ReliableUint64Channel{
        inChan:    make(chan uint64, 0),
        outChan:   make(chan uint64, 0),
        closeChan: make(chan interface{}, 0),
        queue:     list.New(),
        group:     &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *ReliableUint64Channel) Start() {
    routine := func() {
        defer self.group.Done()
        for {
            if self.queue.Len() > 0 {
                e := self.queue.Front()
                out, _ := e.Value.(uint64)
                select {
                case <-self.closeChan:
                    return
                case in := <-self.inChan:
                    self.queue.PushBack(in)
                case self.outChan <- out:
                    self.queue.Remove(e)
                }
            } else {
                select {
                case <-self.closeChan:
                    return
                case in := <-self.inChan:
                    self.queue.PushBack(in)
                }
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *ReliableUint64Channel) Send(in uint64) {
    self.inChan <- in
}

func (self *ReliableUint64Channel) Recv() uint64 {
    out := <-self.outChan
    return out
}

func (self *ReliableUint64Channel) GetInChan() chan<- uint64 {
    return self.inChan
}

func (self *ReliableUint64Channel) GetOutChan() <-chan uint64 {
    return self.outChan
}

func (self *ReliableUint64Channel) Close() {
    self.closeChan <- self
    self.group.Wait()
}

type ReliableInflightEntryChannel struct {
    inChan    chan *InflightEntry
    outChan   chan *InflightEntry
    closeChan chan interface{}
    queue     *list.List
    group     *sync.WaitGroup
}

func NewReliableInflightEntryChannel() *ReliableInflightEntryChannel {
    object := &ReliableInflightEntryChannel{
        inChan:    make(chan *InflightEntry, 0),
        outChan:   make(chan *InflightEntry, 0),
        closeChan: make(chan interface{}, 0),
        queue:     list.New(),
        group:     &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *ReliableInflightEntryChannel) Start() {
    routine := func() {
        defer self.group.Done()
        for {
            if self.queue.Len() > 0 {
                e := self.queue.Front()
                out, _ := e.Value.(*InflightEntry)
                select {
                case <-self.closeChan:
                    return
                case in := <-self.inChan:
                    self.queue.PushBack(in)
                case self.outChan <- out:
                    self.queue.Remove(e)
                }
            } else {
                select {
                case <-self.closeChan:
                    return
                case in := <-self.inChan:
                    self.queue.PushBack(in)
                }
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *ReliableInflightEntryChannel) Send(entry *InflightEntry) {
    self.inChan <- entry
}

func (self *ReliableInflightEntryChannel) Recv() *InflightEntry {
    out := <-self.outChan
    return out
}

func (self *ReliableInflightEntryChannel) GetInChan() chan<- *InflightEntry {
    return self.inChan
}

func (self *ReliableInflightEntryChannel) GetOutChan() <-chan *InflightEntry {
    return self.outChan
}

func (self *ReliableInflightEntryChannel) Close() {
    self.closeChan <- self
    self.group.Wait()
}

// Notifier is use to signal notify to the outside of this module.
type Notifier struct {
    inChan    *ReliableEventChannel
    outChan   chan ev.NotifyEvent
    closeChan chan interface{}
    group     *sync.WaitGroup
}

func NewNotifier() *Notifier {
    object := &Notifier{
        inChan:    NewReliableEventChannel(),
        outChan:   make(chan ev.NotifyEvent, 0),
        closeChan: make(chan interface{}, 1),
        group:     &sync.WaitGroup{},
    }
    object.Start()
    return object
}

func (self *Notifier) Start() {
    self.group.Add(1)
    go func() {
        defer self.group.Done()
        inChan := self.inChan.GetOutChan()
        var event ev.NotifyEvent
        for {
            select {
            case <-self.closeChan:
                return
            case event = <-inChan:
            }

            ne, _ := event.(ev.NotifyEvent)
            select {
            case <-self.closeChan:
                return
            case self.outChan <- ne:
            }
        }
    }()
}

func (self *Notifier) Notify(event ev.NotifyEvent) {
    self.inChan.Send(event)
}

func (self *Notifier) GetNotifyChan() <-chan ev.NotifyEvent {
    return self.outChan
}

func (self *Notifier) Close() {
    self.closeChan <- self
    self.group.Wait()
    self.inChan.Close()
}

// ClientEventListener is a a helper class for listening client response
// in independent go routine.
type ClientEventListener struct {
    eventChan chan ev.Event
    stopChan  chan interface{}
    group     *sync.WaitGroup
}

func NewClientEventListener() *ClientEventListener {
    return &ClientEventListener{
        eventChan: make(chan ev.Event, 1),
        stopChan:  make(chan interface{}),
        group:     &sync.WaitGroup{},
    }
}

func (self *ClientEventListener) Start(fn func(ev.Event)) {
    routine := func() {
        defer self.group.Done()
        for {
            select {
            case <-self.stopChan:
                return
            case event := <-self.eventChan:
                fn(event)
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *ClientEventListener) GetChan() chan ev.Event {
    return self.eventChan
}

func (self *ClientEventListener) Stop() {
    self.stopChan <- self
    self.group.Wait()
}

type Applier struct {
    log          ps.Log
    stateMachine ps.StateMachine
    dispatcher   func(event hsm.Event)
    notifier     *Notifier

    followerCommitChan *ReliableUint64Channel
    leaderCommitChan   *ReliableInflightEntryChannel
    closeChan          chan interface{}
    group              *sync.WaitGroup

    logger logging.Logger
}

func NewApplier(
    log ps.Log,
    stateMachine ps.StateMachine,
    dispatcher func(event hsm.Event),
    notifier *Notifier,
    logger logging.Logger) *Applier {

    object := &Applier{
        log:                log,
        stateMachine:       stateMachine,
        dispatcher:         dispatcher,
        notifier:           notifier,
        followerCommitChan: NewReliableUint64Channel(),
        leaderCommitChan:   NewReliableInflightEntryChannel(),
        closeChan:          make(chan interface{}, 1),
        group:              &sync.WaitGroup{},
        logger:             logger,
    }
    object.Start()
    return object
}

func (self *Applier) Start() {
    routine := func() {
        defer self.group.Done()
        self.ApplyCommitted()
        followerChan := self.followerCommitChan.GetOutChan()
        leaderChan := self.leaderCommitChan.GetOutChan()
        for {
            select {
            case <-self.closeChan:
                return
            case logIndex := <-followerChan:
                self.ApplyLogsUpto(logIndex)
            case inflightEntry := <-leaderChan:
                self.ApplyInflightLog(inflightEntry)
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *Applier) FollowerCommitUpTo(logIndex uint64) {
    self.followerCommitChan.Send(logIndex)
}

func (self *Applier) LeaderCommit(entry *InflightEntry) {
    self.logger.Debug(
        "** applier LeaderCommit(entry, index=%d)", entry.Request.LogEntry.Index)
    self.leaderCommitChan.Send(entry)
}

func (self *Applier) ApplyCommitted() {
    // apply log up to this index if necessary
    committedIndex, err := self.log.CommittedIndex()
    if err != nil {
        self.handleLogError(
            "applier: fail to read committed index of log, error: %#v", err)
        return
    }
    self.ApplyLogsUpto(committedIndex)
}

func (self *Applier) ApplyLogsUpto(index uint64) {
    committedIndex, err := self.log.CommittedIndex()
    if err != nil {
        self.handleLogError(
            "applier: fail to read committed index of log, error: %#v", err)
        return
    }
    if index > committedIndex {
        self.logger.Warning(
            "applier: skip application of uncommitted log, index: %d", index)
        return
    }
    lastAppliedIndex, err := self.log.LastAppliedIndex()
    if err != nil {
        self.handleLogError(
            "applier: fail to read last applied index of log, error: %#v", err)
        return
    }
    if index <= lastAppliedIndex {
        self.logger.Warning(
            "applier: skip application of old log, index: %d", index)
        return
    }
    for i := lastAppliedIndex + 1; i <= index; i++ {
        entry, err := self.log.GetLog(i)
        if err != nil {
            self.handleLogError("applier: fail to read log at index: %d", i)
        }

        if _, err = self.ApplyLogEntry(entry); err != nil {
            self.handleLogError("applier: fail to apply log at index: %d", i)
            return
        }
        self.notifier.Notify(ev.NewNotifyApplyEvent(entry.Term, entry.Index))
    }
}

func (self *Applier) ApplyLogEntry(
    entry *ps.LogEntry) (result []byte, err error) {

    switch entry.Type {
    // TODO add other types
    case ps.LogCommand:
        result = self.stateMachine.Apply(entry.Data)
    case ps.LogNoop:
        // nothing to do
    case ps.LogMemberChange:
        // nothing to do here
    default:
        self.logger.Error(
            "unknown log entry type: %d, index: %s", entry.Type, entry.Index)
    }
    err = self.log.StoreLastAppliedIndex(entry.Index)
    return result, err
}

func (self *Applier) ApplyInflightLog(entry *InflightEntry) {
    self.logger.Debug(
        "** applier ApplyInflightLog(entry, index=%d)", entry.Request.LogEntry.Index)
    committedIndex, err := self.log.CommittedIndex()
    if err != nil {
        self.handleLogError(
            "applier: fail to read committed index of log, error: %#v", err)
        return
    }
    logIndex := entry.Request.LogEntry.Index
    if logIndex > committedIndex {
        self.logger.Warning(
            "applier: skip application of uncommitted log, index: %d", logIndex)
        return
    }
    lastAppliedIndex, err := self.log.LastAppliedIndex()
    if err != nil {
        self.handleLogError(
            "applier: fail to read last applied index of log, error: %#v", err)
        return
    }
    if logIndex <= lastAppliedIndex {
        self.logger.Warning(
            "applier: skip application of old log, index: %d", logIndex)
        return
    }
    if logIndex != lastAppliedIndex+1 {
        self.logger.Warning(
            "applier: skip application of non-next log, index: %d", logIndex)
        return
    }

    result, err := self.ApplyLogEntry(entry.Request.LogEntry)
    if err != nil {
        self.handleLogError(
            "applier: fail to apply log at index: %d", logIndex)
        return
    }
    self.logger.Debug("** applier apply log at index: %d", logIndex)

    if entry.Request.LogEntry.Type == ps.LogMemberChange {
        // don't response client here
        message := &ev.LeaderForwardMemberChangePhase{
            Conf:       entry.Request.LogEntry.Conf,
            ResultChan: entry.Request.ResultChan,
        }
        self.dispatcher(ev.NewLeaderForwardMemberChangePhaseEvent(message))
    } else {
        // response client immediately
        response := &ev.ClientResponse{
            Success: true,
            Data:    result,
        }
        entry.Request.ResultChan <- ev.NewClientResponseEvent(response)
        self.logger.Debug(
            "** applier response client for index: %d to chan: %#v",
            logIndex, entry.Request.ResultChan)
    }

    self.notifier.Notify(
        ev.NewNotifyApplyEvent(entry.Request.LogEntry.Term, logIndex))
}

func (self *Applier) handleLogError(format string, args ...interface{}) {
    errorMessage := fmt.Sprintf(format, args...)
    self.logger.Error(errorMessage)
    event := ev.NewPersistErrorEvent(errors.New(errorMessage))
    self.dispatcher(event)
}

func (self *Applier) Close() {
    self.closeChan <- self
    self.group.Wait()
}

// Min returns the minimum.
func Min(a, b uint64) uint64 {
    if a <= b {
        return a
    }
    return b
}

// Max returns the maximum.
func Max(a, b uint64) uint64 {
    if a >= b {
        return a
    }
    return b
}

func GetPeers(
    localAddr *ps.ServerAddress, conf *ps.Config) *ps.ServerAddressSlice {

    addrs := make(
        []*ps.ServerAddress, 0, ps.Len(conf.Servers)+ps.Len(conf.NewServers))
    if conf.Servers != nil {
        for _, addr := range conf.Servers.Addresses {
            if ps.MultiAddrNotEqual(addr, localAddr) {
                addrs = append(addrs, addr)
            }
        }
    }
    if conf.NewServers != nil {
        for _, addr := range conf.NewServers.Addresses {
            if ps.MultiAddrNotEqual(addr, localAddr) {
                addrs = append(addrs, addr)
            }
        }
    }
    return &ps.ServerAddressSlice{
        Addresses: addrs,
    }
}

func AddrSliceToMap(
    addrSlice *ps.ServerAddressSlice) map[*ps.ServerAddress]Peer {

    result := make(map[*ps.ServerAddress]Peer, addrSlice.Len())
    for _, addr := range addrSlice.Addresses {
        result[addr] = nil
    }
    return result
}

// MapSetMinus calculates the difference of two map, and returns
// the result of s1 - s2.
func MapSetMinus(
    s1 map[*ps.ServerAddress]Peer,
    s2 map[*ps.ServerAddress]Peer) []*ps.ServerAddress {

    diff := make([]*ps.ServerAddress, 0)
    for addr, _ := range s1 {
        if _, ok := s2[addr]; !ok {
            diff = append(diff, addr)
        }
    }
    return diff
}

func AddrsString(addrs []*ps.ServerAddress) []string {
    result := make([]string, 0, len(addrs))
    for _, addr := range addrs {
        result = append(result, addr.String())
    }
    return result
}

func EntriesInfo(entries []*ps.LogEntry) []string {
    result := make([]string, 0, len(entries))
    for _, entry := range entries {
        info := fmt.Sprintf("Term: %d Index: %d", entry.Term, entry.Index)
        result = append(result, info)
    }
    return result
}

func ParallelDo(todo []func()) {
    todoGroup := sync.WaitGroup{}
    for _, f1 := range todo {
        f := f1
        todoGroup.Add(1)
        go func() {
            defer todoGroup.Done()
            f()
        }()
    }
    todoGroup.Wait()
}

func ParallelClose(closers []io.Closer) {
    todoGroup := sync.WaitGroup{}
    for _, closer1 := range closers {
        closer := closer1
        todoGroup.Add(1)
        go func() {
            defer todoGroup.Done()
            closer.Close()
        }()
    }
    todoGroup.Wait()
}
