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
    indexMap   map[uint64]uint64
    logEntries []*LogEntry
    logLock    sync.RWMutex
}

func NewMemoryLog() *MemoryLog {
    return &MemoryLog{
        indexMap:   make(map[uint64]uint64),
        logEntries: make([]*LogEntry, 0),
    }
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

func (self *MemoryLog) FirstTerm() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[0]
    return entry.Term, nil
}

func (self *MemoryLog) LastIndex() (uint64, error) {
    self.logLock.RLock()
    defer self.logLock.RUnlock()

    if len(self.logEntries) == 0 {
        return 0, nil
    }

    entry := self.logEntries[len(self.logEntries)-1]
    return entry.Index, nil
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

    storedIndex, ok := self.indexMap[index]
    if !ok {
        return errors.New("no such index")
    }
    // truncate the real storage
    self.logEntries = self.logEntries[:storedIndex+1]
    // cleanup and update the indexes
    for k, _ := range self.indexMap {
        if k > index {
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
    term, index uint64, Servers []byte) *MemorySnapshotInstance {

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
    term, index uint64, Servers []byte) (SnapshotWriter, error) {

    self.lock.Lock()
    defer self.lock.Unlock()

    instance := NewMemorySnapshotInstance(term, index, Servers)
    self.snapshotList.PushBack(instance)
    return NewMemorySnapshotWriter(instance), nil
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
}
