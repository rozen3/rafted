package persist

import (
    "bytes"
    "container/list"
    "encoding/binary"
    "errors"
    "fmt"
    "io"
    "sync"
)

type MemoryLog struct {
    indexMap         map[uint64]uint64
    logEntries       []*LogEntry
    lastAppliedIndex uint64
    committedIndex   uint64
    logLock          sync.RWMutex
}

func NewMemoryLog() *MemoryLog {
    return &MemoryLog{
        indexMap:         make(map[uint64]uint64),
        logEntries:       make([]*LogEntry, 0),
        lastAppliedIndex: 0,
        committedIndex:   0,
    }
}

func (self *MemoryLog) FirstTerm() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[0]
    return entry.Term, nil
}

func (self *MemoryLog) FirstIndex() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()
    return self.firstIndex()
}

func (self *MemoryLog) firstIndex() (uint64, error) {
    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[0]
    return entry.Index, nil
}

func (self *MemoryLog) FirstEntryInfo() (uint64, uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, 0, nil
    }

    entry := self.logEntries[0]
    return entry.Term, entry.Index, nil
}

func (self *MemoryLog) LastTerm() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[len(self.logEntries)-1]
    return entry.Term, nil
}

func (self *MemoryLog) LastIndex() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()
    return self.lastIndex()
}

func (self *MemoryLog) lastIndex() (uint64, error) {
    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[len(self.logEntries)-1]
    return entry.Index, nil
}

func (self *MemoryLog) LastEntryInfo() (uint64, uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, 0, nil
    }

    entry := self.logEntries[len(self.logEntries)-1]
    return entry.Term, entry.Index, nil
}

func (self *MemoryLog) CommittedIndex() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()
    return self.committedIndex, nil
}

func (self *MemoryLog) StoreCommittedIndex(index uint64) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()
    lastLogIndex, err := self.lastIndex()
    if err != nil {
        return err
    }
    if (index > self.lastAppliedIndex) && (index <= lastLogIndex) {
        self.committedIndex = index
        return nil
    }
    return errors.New("invalid index")
}

func (self *MemoryLog) LastAppliedIndex() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()
    return self.lastAppliedIndex, nil
}

func (self *MemoryLog) StoreLastAppliedIndex(index uint64) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()
    if index <= self.committedIndex {
        self.lastAppliedIndex = index
        return nil
    }
    return errors.New("invalid index")
}

func (self *MemoryLog) GetLog(index uint64) (*LogEntry, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    storedIndex, ok := self.indexMap[index]
    if !ok {
        return nil, errors.New("no such index")
    }
    entry := self.logEntries[storedIndex]
    return entry, nil
}

func (self *MemoryLog) GetLogInRange(
    fromIndex uint64, toIndex uint64) ([]*LogEntry, error) {

    self.logLock.RLock()
    defer self.logLock.RUnlock()

    fromStoredIndex, ok := self.indexMap[fromIndex]
    if !ok {
        return nil, errors.New("no such from index")
    }
    toStoredIndex, ok := self.indexMap[toIndex]
    if !ok {
        return nil, errors.New("no such to index")
    }
    result := make([]*LogEntry, 0, toStoredIndex-fromStoredIndex+1)
    for i := fromStoredIndex; i <= toStoredIndex; i++ {
        result = append(result, self.logEntries[i])
    }
    return result, nil
}

func (self *MemoryLog) StoreLog(log *LogEntry) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()
    return self.storeLog(log)
}

func (self *MemoryLog) storeLog(log *LogEntry) error {
    storedIndex := len(self.logEntries)
    self.logEntries = append(self.logEntries, log)
    self.indexMap[log.Index] = uint64(storedIndex)
    return nil
}

func (self *MemoryLog) StoreLogs(logs []*LogEntry) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()

    for _, log := range logs {
        err := self.storeLog(log)
        if err != nil {
            return err
        }
    }
    return nil
}

func (self *MemoryLog) TruncateBefore(index uint64) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()

    if index > self.lastAppliedIndex {
        return errors.New("invalid index after lastAppliedIndex")
    }

    storedIndex, ok := self.indexMap[index]
    if !ok {
        return errors.New("no such index")
    }
    // truncate the real storage
    truncatedLength := storedIndex + 1
    self.logEntries = self.logEntries[truncatedLength:]
    // cleanup and update the indexes
    for k, v := range self.indexMap {
        if k <= index {
            delete(self.indexMap, k)
        } else {
            self.indexMap[k] = v - truncatedLength
        }
    }
    return nil
}

func (self *MemoryLog) TruncateAfter(index uint64) error {
    self.logLock.Lock()
    defer self.logLock.Unlock()

    if index <= self.committedIndex {
        return errors.New("invalid index before committedIndex")
    }

    storedIndex, ok := self.indexMap[index]
    if !ok {
        return errors.New("no such index")
    }
    // truncate the real storage
    self.logEntries = self.logEntries[:storedIndex]
    // cleanup and update the indexes
    for k, _ := range self.indexMap {
        if k >= index {
            delete(self.indexMap, k)
        }
    }
    return nil
}

type MemorySnapshot struct {
    Meta *SnapshotMeta
    Data *list.List
}

type MemorySnapshotWriter struct {
    stateMachine *MemoryStateMachine
    snapshot     *MemorySnapshot
    lock         sync.Mutex
}

func NewMemorySnapshotWriter(
    stateMachine *MemoryStateMachine,
    snapshot *MemorySnapshot) *MemorySnapshotWriter {

    return &MemorySnapshotWriter{
        stateMachine: stateMachine,
        snapshot:     snapshot,
    }
}

func (self *MemorySnapshotWriter) Write(p []byte) (int, error) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.instance.Data.PushBack(p)
    return len(p), nil
}

func (self *MemorySnapshotWriter) Close() error {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.instance.Size = self.instance.Data.Len()
    self.stateMachine.addSnapshot(self.instance)
    return nil
}

func (self *MemorySnapshotWriter) ID() string {
    return self.instance.Meta.ID
}

func (self *MemorySnapshotWriter) Cancel() error {
    // empty body
    return nil
}

type MemoryStateMachine struct {
    dataList     *list.List
    dataLock     sync.Mutex
    snapshotList *list.List
    snapshotLock sync.RWMutex
}

func NewMemoryStateMachine() *MemoryStateMachine {
    dataList := list.New()
    snapshotList := list.New()
    return &MemoryStateMachine{
        dataList:     dataList,
        snapshotList: snapshotList,
    }
}

func (self *MemoryStateMachine) Data() *list.List {
    self.dataLock.Lock()
    defer self.dataLock.Unlock()
    return self.dataList
}

func (self *MemoryStateMachine) Apply(p []byte) []byte {
    self.dataLock.Lock()
    defer self.dataLock.Unlock()
    self.dataList.PushBack(p)
    return p
}

func (self *MemoryStateMachine) getID(term, index uint64) string {
    id := fmt.Sprintf(
        "Term:%d Index:%d", lastIncludedTerm, lastIncludedIndex)
    return id
}

func (self *MemoryStateMachine) MakeSnapshot(
    lastIncludedTerm uint64,
    lastIncludedIndex uint64,
    conf *Config) (id string, err error) {

    self.datalock.Lock()
    self.snapshotLock.Lock()
    defer self.dataLock.Unlock()
    defer self.snapshotLock.Unlock()
    // construct the metadata
    id := self.getID(lastIncludedTerm, lastIncludedIndex)
    meta := &SnapshotMeta{
        ID:                id,
        LastIncludedTerm:  lastIncludedTerm,
        LastIncludedIndex: lastIncludedIndex,
        Size:              uint64(self.dataList.Len()),
        Conf:              conf,
    }
    // copy all data
    lst := list.New()
    lst.PushBackList(self.dataList)
    snapshot := &MemorySnapshot{
        Meta: meta,
        Data: lst,
    }
    self.snapshotList.PushBack(snapshot)
    return id, nil
}

func (self *MemoryStateMachine) MakeEmptySnapshot(
    lastIncludedTerm uint64,
    lastIncludedIndex uint64,
    conf *Config) (SnapshotWriter, error) {

    self.snapshotLock.Lock()
    defer self.snapshotLock.Unlock()
    id := self.getID(lastIncludedTerm, lastIncludedIndex)
    meta := &SnapshotMeta{
        ID:                id,
        LastIncludedTerm:  lastIncludedTerm,
        LastIncludedIndex: lastIncludedIndex,
        Size:              0,
        Conf:              conf,
    }
    snapshot := &MemorySnapshot{
        Meta: meta,
        Data: list.New(),
    }
    writer := &MemorySnapshotWriter{
        stateMachine: self,
        snapshot:     snapshot,
    }
    return writer, nil
}

func (self *MemoryStateMachine) addSnapshot(snapshot *MemorySnapshot) {
    self.snapshotLock.Lock()
    defer self.snapshotLock.Unlock()
    self.snapshotList.PushBack(snapshot)
}

func (self *MemoryStateMachine) RestoreFromSnapshot(id string) error {
    self.lock.Lock()
    self.snapshotLock.RLock()
    defer self.lock.Unlock()
    defer self.snapshotLock.RUnlock()

    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        snapshot, _ := e.Value.(*MemorySnapshot)
        if snapshot.Meta.ID == id {
            self.dataList.Init()
            self.dataList.PushBackList(snapshot.Data)
            return nil
        }
    }
    return ErrorSnapshotNotFound
}

func (self *MemoryStateMachine) LastSnapshotInfo() (*SnapshotMeta, error) {
    self.snapshotLock.RLock()
    defer self.snapshotLock.RUnlock()
    if self.snapshotList.Len() == 0 {
        return nil, ErrorNoSnapshot
    }
    elem := self.snapshotList.Back()
    snapshot, _ := elem.Value.(*MemorySnapshot)
    return snapshot.Meta, nil
}

func (self *MemoryStateMachine) AllSnapshotInfo() ([]*SnapshotMeta, error) {
    self.snapshotLock.RLock()
    defer self.snapshotLock.RUnlock()
    length := self.snapshotList.Len()
    if length == 0 {
        return nil, ErrorNoSnapshot
    }
    result := make([]*SnapshotMeta, 0, length)
    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        snapshot, _ := e.Value.(*MemorySnapshot)
        result = append(result, snapshot.Meta)
    }
    return result, nil
}

func (self *MemoryStateMachine) OpenSnapshot(
    id string) (*SnapshotMeta, io.ReadCloser, error) {

    self.snapshotLock.RLock()
    defer self.snapshotLock.RUnlock()
    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        snapshot, _ := e.Value.(*MemorySnapshot)
        if snapshot.Meta.ID == id {
            allData := make([]byte, 0)
            for elem := snapshot.Data.Front(); elem != nil; elem = elem.Next() {
                data, _ := elem.([]byte)
                allData = append(allData, data)
            }
            readerCloser := NewReaderCloserWrapper(bytes.NewReader(allData))
            return snapshot.Meta, readerCloser, nil
        }
    }
    return nil, nil, ErrorSnapshotNotFound
}

func (self *MemoryStateMachine) DeleteSnapshot(id string) error {
    self.snapshotLock.Lock()
    defer self.snapshotLock.Unlock()
    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        snapshot, _ := e.Value.(*MemorySnapshot)
        if snapshot.Meta.ID == id {
            self.snapshotList.Remove(e)
            return nil
        }
    }
    return ErrorSnapshotNotFound
}

type MemoryConfigManager struct {
    configs *list.List
    lock    sync.RWMutex
}

func NewMemoryConfigManager(
    firstLogIndex uint64, conf *Config) *MemoryConfigManager {

    lst := list.New()
    meta := &ConfigMeta{
        FromLogIndex: firstLogIndex,
        ToLogIndex:   0,
        Conf:         conf,
    }
    lst.PushBack(meta)
    return &MemoryConfigManager{
        configs: lst,
    }
}

func (self *MemoryConfigManager) Push(logIndex uint64, conf *Config) error {
    self.lock.Lock()
    defer self.lock.Unlock()
    elem := self.configs.Back()
    meta, _ := elem.Value.(*ConfigMeta)
    meta.ToLogIndex = logIndex - 1

    newMeta := &ConfigMeta{
        FromLogIndex: logIndex,
        ToLogIndex:   0,
        Conf:         conf,
    }
    self.configs.PushBack(newMeta)
    return nil
}

func (self *MemoryConfigManager) RNth(n uint32) (*Config, error) {
    self.lock.RLock()
    defer self.lock.RUnlock()
    elem := self.configs.Back()
    for i := uint32(0); (i < n) && (elem != nil); i++ {
        elem = elem.Prev()
    }
    if elem == nil {
        return nil, errors.New("n out of bound")
    }
    meta, _ := elem.Value.(*ConfigMeta)
    return meta.Conf, nil
}

func (self *MemoryConfigManager) PreviousOf(
    logIndex uint64) (*ConfigMeta, error) {

    self.lock.RLock()
    defer self.lock.RUnlock()
    e := self.configs.Back()
    for ; e != nil; e = e.Prev() {
        meta, _ := e.Value.(*ConfigMeta)
        if meta.FromLogIndex < logIndex {
            return meta, nil
        }
    }
    return nil, errors.New("index out of bound")
}

func (self *MemoryConfigManager) ListAfter(logIndex uint64) ([]*ConfigMeta, error) {
    self.lock.RLock()
    defer self.lock.RUnlock()
    e := self.configs.Back()
    found := false
    count := 0
    for ; e != nil; e = e.Prev() {
        meta, _ := e.Value.(*ConfigMeta)
        count++
        if (meta.FromLogIndex <= logIndex) &&
            ((meta.ToLogIndex == 0) || (logIndex <= meta.ToLogIndex)) {
            found = true
            break
        }
    }
    if found {
        result := make([]*ConfigMeta, 0, count)
        for el := e; el != nil; el = el.Next() {
            meta, _ := el.Value.(*ConfigMeta)
            result = append(result, meta)
        }
        return result, nil
    }
    return nil, errors.New("index out of bound")
}

func (self *MemoryConfigManager) TruncateBefore(logIndex uint64) error {
    self.lock.Lock()
    defer self.lock.Unlock()
    for e := self.configs.Front(); e != nil; e = e.Next() {
        meta, _ := e.Value.(*ConfigMeta)
        if logIndex > meta.ToLogIndex {
            continue
        }
        if logIndex == meta.ToLogIndex {
            if e == self.configs.Back() {
                ListTruncateHead(self.configs, e)
                meta.FromLogIndex = logIndex
                meta.ToLogIndex = 0
            } else {
                ListTruncateHead(self.configs, e)
                self.configs.Remove(e)
            }
        } else {
            ListTruncateHead(self.configs, e)
            meta.FromLogIndex = logIndex
        }
        return nil
    }
    return errors.New("index out of bound")
}

func (self *MemoryConfigManager) TruncateAfter(
    logIndex uint64) (*Config, error) {

    self.lock.Lock()
    defer self.lock.Unlock()
    for e := self.configs.Back(); e != nil; e = e.Prev() {
        meta, _ := e.Value.(*ConfigMeta)
        if logIndex < meta.FromLogIndex {
            continue
        }
        if logIndex == meta.FromLogIndex {
            if e == self.configs.Front() {
                ListTruncate(self.configs, e.Next())
                meta.ToLogIndex = 0
                return meta.Conf, nil
            } else {
                prev := e.Prev()
                meta, _ := prev.Value.(*ConfigMeta)
                ListTruncate(self.configs, e)
                return meta.Conf, nil
            }
        } else {
            ListTruncate(self.configs, e.Next())
            meta.ToLogIndex = 0
            return meta.Conf, nil
        }
    }
    return nil, errors.New("index out of bound")
}
