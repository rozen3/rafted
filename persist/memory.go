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
    if (index > self.lastAppliedIndex) && (index < lastLogIndex) {
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
    firstLogIndex, err := self.FirstIndex()
    if err != nil {
        return err
    }
    if (index > firstLogIndex) && (index < self.committedIndex) {
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

type MemorySnapshotInstance struct {
    Meta *SnapshotMeta
    Data []byte
    lock sync.Mutex
}

func NewMemorySnapshotInstance(
    term, index uint64, Servers []ServerAddr) *MemorySnapshotInstance {

    return &MemorySnapshotInstance{
        Meta: &SnapshotMeta{
            ID:                Timestamp(),
            LastIncludedTerm:  term,
            LastIncludedIndex: index,
            Servers:           Servers,
        },
        Data: make([]byte, 0),
    }
}

func (self *MemorySnapshotInstance) SetSize(size uint64) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.Meta.Size = size
}

func (self *MemorySnapshotInstance) SetData(data []byte) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.Data = data
    self.SetSize(uint64(len(data)))
}

type MemorySnapshotManager struct {
    snapshotList *list.List
    lock         sync.RWMutex
}

func NewMemorySnapshotManager() *MemorySnapshotManager {
    lst := list.New()
    return &MemorySnapshotManager{
        snapshotList: lst,
    }
}

func (self *MemorySnapshotManager) Create(
    term, index uint64, Servers []ServerAddr) (SnapshotWriter, error) {

    self.lock.Lock()
    defer self.lock.Unlock()

    instance := NewMemorySnapshotInstance(term, index, Servers)
    self.snapshotList.PushBack(instance)
    return NewMemorySnapshotWriter(instance), nil
}

func (self *MemorySnapshotManager) LastSnapshotInfo() (uint64, uint64, error) {
    if self.snapshotList.Len() == 0 {
        return 0, 0, nil
    }
    elem := self.snapshotList.Back()
    meta, _ := elem.Value.(*SnapshotMeta)
    return meta.LastIncludedTerm, meta.LastIncludedIndex, nil
}

func (self *MemorySnapshotManager) List() ([]*SnapshotMeta, error) {
    self.lock.RLock()
    defer self.lock.RUnlock()

    total := self.snapshotList.Len()
    allMeta := make([]*SnapshotMeta, 0, total)
    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        instance, ok := e.Value.(*MemorySnapshotInstance)
        if !ok {
            return allMeta, errors.New("corrupted format")
        }
        allMeta = append(allMeta, instance.Meta)
    }
    return allMeta, nil
}

func (self *MemorySnapshotManager) Open(
    id string) (*SnapshotMeta, io.ReadCloser, error) {

    self.lock.RLock()
    defer self.lock.RUnlock()

    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        instance, ok := e.Value.(*MemorySnapshotInstance)
        if !ok {
            return nil, nil, errors.New("corrupted format")
        }
        if instance.Meta.ID == id {
            readerCloser := NewReaderCloserWrapper(
                bytes.NewReader(instance.Data))
            return instance.Meta, readerCloser, nil
        }
    }
    return nil, nil, errors.New(fmt.Sprintf("no snapshot for id: %s", id))
}

func (self *MemorySnapshotManager) Delete(id string) error {
    self.lock.RLock()
    defer self.lock.RUnlock()

    for e := self.snapshotList.Front(); e != nil; e = e.Next() {
        instance, ok := e.Value.(*MemorySnapshotInstance)
        if !ok {
            return errors.New("corrupted format")
        }
        if instance.Meta.ID == id {
            self.snapshotList.Remove(e)
            return nil
        }
    }
    return errors.New(fmt.Sprintf("no snapshot for id: %s", id))
}

type MemorySnapshotWriter struct {
    instance *MemorySnapshotInstance
    data     []byte
    lock     sync.Mutex
}

func NewMemorySnapshotWriter(
    instance *MemorySnapshotInstance) *MemorySnapshotWriter {

    return &MemorySnapshotWriter{
        instance: instance,
        data:     make([]byte, 0),
    }
}

func (self *MemorySnapshotWriter) Write(p []byte) (int, error) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.data = append(self.data, p...)
    return len(p), nil
}

func (self *MemorySnapshotWriter) Close() error {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.instance.SetData(self.data)
    return nil
}

func (self *MemorySnapshotWriter) ID() string {
    return self.instance.Meta.ID
}

func (self *MemorySnapshotWriter) Cancel() error {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.data = make([]byte, 0)
    return nil
}

type MemoryStateMachine struct {
    data *list.List
    lock sync.Mutex
}

func NewMemoryStateMachine() *MemoryStateMachine {
    lst := list.New()
    return &MemoryStateMachine{
        data: lst,
    }
}

func (self *MemoryStateMachine) Apply(p []byte) []byte {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.data.PushBack(p)
    return p
}

func (self *MemoryStateMachine) MakeSnapshot() (Snapshot, error) {
    self.lock.Lock()
    defer self.lock.Unlock()
    // copy all data
    lst := list.New()
    lst.PushBackList(self.data)
    return NewMemorySnapshot(lst), nil
}

func (self *MemoryStateMachine) Restore(readerCloser io.ReadCloser) error {
    self.lock.Lock()
    defer self.lock.Unlock()
    defer readerCloser.Close()

    byteReader := NewByteReaderWrapper(readerCloser)
    length, err := binary.ReadUvarint(byteReader)
    if err != nil {
        return nil
    }
    data := list.New()
    var i uint64 = 0
    for ; i < length; i++ {
        byteSize, err := binary.ReadUvarint(byteReader)
        if err != nil {
            return err
        }
        b := make([]byte, byteSize)
        n, err := binary.ReadUvarint(byteReader)
        if err != nil {
            return err
        }
        if n != uint64(len(b)) {
            return errors.New("size missmatched")
        }
        data.PushBack(b)
    }
    self.data = data
    return nil
}

type MemorySnapshot struct {
    data *list.List
    lock sync.Mutex
}

func NewMemorySnapshot(data *list.List) *MemorySnapshot {
    return &MemorySnapshot{
        data: data,
    }
}

func (self *MemorySnapshot) Persist(writer SnapshotWriter) error {
    self.lock.Lock()
    defer self.lock.Unlock()

    length := uint64(self.data.Len())
    b := Uint64ToBytes(length)
    if _, err := writer.Write(b); err != nil {
        return err
    }
    for e := self.data.Front(); e != nil; e = e.Next() {
        v, ok := e.Value.([]byte)
        if !ok {
            return errors.New("invalid type in data")
        }
        // write the length of data first
        length = uint64(len(v))
        b = Uint64ToBytes(length)
        if _, err := writer.Write(b); err != nil {
            return err
        }
        // write the data
        if _, err := writer.Write(v); err != nil {
            return err
        }
    }
    writer.Close()
    return nil
}

func (self *MemorySnapshot) Release() {
    self.lock.Lock()
    defer self.lock.Unlock()

    // empty body
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
    for ; e != nil; e = e.Prev() {
        meta, _ := e.Value.(*ConfigMeta)
        if (meta.FromLogIndex <= logIndex) &&
            (logIndex <= meta.ToLogIndex) {
            found = true
        }
    }
    if found {
        result := make([]*ConfigMeta, 0)
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
