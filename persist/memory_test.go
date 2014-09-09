package persist

import (
    "github.com/hhkbp2/rafted/str"
    "github.com/hhkbp2/testify/assert"
    "io"
    "testing"
)

var (
    testData = []byte(str.RandomString(100))
)

func ValueOf(value uint64, _ error) uint64 {
    return value
}

func checkFirstEntryInfo(t *testing.T, log Log, term, index uint64) {
    assert.Equal(t, term, ValueOf(log.FirstTerm()))
    assert.Equal(t, index, ValueOf(log.FirstIndex()))
    term, index, err := log.FirstEntryInfo()
    assert.Nil(t, err)
    assert.Equal(t, term, term)
    assert.Equal(t, index, index)
}

func checkLastEntryInfo(t *testing.T, log Log, term, index uint64) {
    assert.Equal(t, term, ValueOf(log.LastTerm()))
    assert.Equal(t, index, ValueOf(log.LastIndex()))
    term, index, err := log.LastEntryInfo()
    assert.Nil(t, err)
    assert.Equal(t, term, term)
    assert.Equal(t, index, index)
}

func getTestLogEntry(term, index uint64) *LogEntry {
    entry := &LogEntry{
        Term:  term,
        Index: index,
        Type:  LogCommand,
        Data:  testData,
        Conf: &Config{
            Servers:    SetupMemoryServerAddrs(5),
            NewServers: nil,
        },
    }
    return entry
}

func TestMemoryLog(t *testing.T) {
    // test construction
    log := NewMemoryLog()
    checkFirstEntryInfo(t, log, 0, 0)
    checkLastEntryInfo(t, log, 0, 0)
    assert.Equal(t, 0, ValueOf(log.CommittedIndex()))
    assert.Equal(t, 0, ValueOf(log.LastAppliedIndex()))
    // test GetLog()
    entry, err := log.GetLog(1)
    assert.NotNil(t, err)
    entries, err := log.GetLogInRange(1, 100)
    assert.NotNil(t, err)
    // test StoreLog()
    term := uint64(100)
    index := uint64(23355)
    entry = getTestLogEntry(term, index)
    err = log.StoreLog(entry)
    assert.Nil(t, err)
    checkFirstEntryInfo(t, log, term, index)
    checkLastEntryInfo(t, log, term, index)
    // test GetLog()
    e, err := log.GetLog(index)
    assert.Nil(t, err)
    assert.Equal(t, entry, e)
    // test GetLogInRange()
    entries, err = log.GetLogInRange(index, index)
    assert.Nil(t, err)
    assert.Equal(t, 1, len(entries))
    assert.Equal(t, entry, entries[0])
    // test CommittedIndex
    err = log.StoreCommittedIndex(index + 1)
    assert.NotNil(t, err)
    assert.Equal(t, 0, ValueOf(log.CommittedIndex()))
    committedIndex := uint64(index - 1000)
    err = log.StoreCommittedIndex(committedIndex)
    assert.Nil(t, err)
    assert.Equal(t, committedIndex, ValueOf(log.CommittedIndex()))
    // test LastAppliedIndex
    err = log.StoreLastAppliedIndex(index + 1)
    assert.NotNil(t, err)
    assert.Equal(t, 0, ValueOf(log.LastAppliedIndex()))
    lastAppliedIndex := uint64(index - 1001)
    err = log.StoreLastAppliedIndex(lastAppliedIndex)
    assert.Nil(t, err)
    assert.Equal(t, lastAppliedIndex, ValueOf(log.LastAppliedIndex()))
    // test StoreLogs()
    entries = []*LogEntry{
        getTestLogEntry(term, index+1),
        getTestLogEntry(term+1, index+2),
    }
    err = log.StoreLogs(entries)
    assert.Nil(t, err)
    readEntries, err := log.GetLogInRange(index+1, index+2)
    assert.Nil(t, err)
    assert.Equal(t, entries, readEntries)
    // test TruncateBefore()
    log.StoreCommittedIndex(index)
    log.StoreLastAppliedIndex(index)
    err = log.TruncateBefore(index)
    assert.Nil(t, err)
    e, err = log.GetLog(index)
    assert.NotNil(t, err)
    // test TruncateAfter()
    err = log.TruncateAfter(index + 1)
    assert.Nil(t, err)
    e, err = log.GetLog(index + 1)
    assert.NotNil(t, err)
    e, err = log.GetLog(index + 2)
    assert.NotNil(t, err)
}

func TestMemorySnapshotManager(t *testing.T) {
    term := uint64(100)
    index := uint64(23355)
    servers := SetupMemoryServerAddrs(5)
    data := testData
    manager := NewMemorySnapshotManager()
    metas, err := manager.List()
    assert.Equal(t, 0, len(metas))
    // test Create()
    writer, err := manager.Create(term, index, servers)
    assert.Nil(t, err)
    writer.Write(data)
    metas, err = manager.List()
    assert.Nil(t, err)
    assert.Equal(t, 0, len(metas))
    readTerm, readIndex, err := manager.LastSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, 0, readTerm)
    assert.Equal(t, 0, readIndex)
    id := writer.ID()
    // test writer Close()
    writer.Close()
    // test List()
    metas, err = manager.List()
    assert.Nil(t, err)
    assert.Equal(t, 1, len(metas))
    // test LastSnapshotInfo()
    readTerm, readIndex, err = manager.LastSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, term, readTerm)
    assert.Equal(t, index, readIndex)
    // test Open()
    meta, reader, err := manager.Open(id)
    assert.Nil(t, err)
    defer reader.Close()
    assert.Equal(t, term, meta.LastIncludedTerm)
    assert.Equal(t, index, meta.LastIncludedIndex)
    assert.Equal(t, servers, meta.Servers)
    readData := make([]byte, 0, len(data))
    for {
        p := make([]byte, len(data))
        n, err := reader.Read(p)
        if (err == nil) || (err == io.EOF) {
            readData = append(readData, p[:n]...)
        }
        if (err == io.EOF) || (err != nil) {
            break
        }
    }
    assert.Equal(t, meta.Size, len(readData))
    assert.Equal(t, data, readData)
    // test Delete()
    err = manager.Delete(id)
    assert.Nil(t, err)
    metas, err = manager.List()
    assert.Nil(t, err)
    assert.Equal(t, 0, len(metas))
}

func TestMemoryStateMachine(t *testing.T) {
    data1 := []byte(str.RandomString(100))
    data2 := []byte(str.RandomString(50))
    stateMachine := NewMemoryStateMachine()
    // test Apply()
    d := stateMachine.Apply(data1)
    assert.Equal(t, data1, d)
    d = stateMachine.Apply(data2)
    assert.Equal(t, data2, d)
    // test MakeSnapshot()
    term := uint64(100)
    index := uint64(23355)
    servers := SetupMemoryServerAddrs(5)
    manager := NewMemorySnapshotManager()
    writer, err := manager.Create(term, index, servers)
    id := writer.ID()
    assert.Nil(t, err)
    snapshot, err := stateMachine.MakeSnapshot()
    assert.Nil(t, err)
    // test snapshot Persist()
    err = snapshot.Persist(writer)
    assert.Nil(t, err)
    snapshot.Release()
    // test Restore()
    meta, reader, err := manager.Open(id)
    assert.Nil(t, err)
    assert.True(t, meta.Size > 0)
    stateMachine2 := NewMemoryStateMachine()
    err = stateMachine2.Restore(reader)
    assert.Nil(t, err)
    assert.Equal(t, stateMachine.Data(), stateMachine2.Data())
}

func TestMemoryConfigManager(t *testing.T) {
    firstIndex := uint64(333)
    conf := &Config{
        Servers:    SetupMemoryServerAddrs(5),
        NewServers: SetupMemoryServerAddrs(5),
    }
    manager := NewMemoryConfigManager(firstIndex, conf)
    // test RNth()
    c, err := manager.RNth(1)
    assert.NotNil(t, err)
    c, err = manager.RNth(0)
    assert.Nil(t, err)
    assert.Equal(t, conf, c)
    // test PreviousOf()
    meta, err := manager.PreviousOf(firstIndex)
    assert.NotNil(t, err)
    meta, err = manager.PreviousOf(firstIndex + 1)
    assert.Nil(t, err)
    assert.Equal(t, conf, meta.Conf)
    // test Push()
    nextIndex := uint64(456)
    conf2 := &Config{
        Servers:    conf.NewServers,
        NewServers: nil,
    }
    err = manager.Push(nextIndex, conf2)
    assert.Nil(t, err)
    // test ListAfter()
    metas, err := manager.ListAfter(firstIndex)
    assert.Nil(t, err)
    assert.Equal(t, 2, len(metas))
    assert.Equal(t, conf, metas[0].Conf)
    assert.Equal(t, conf2, metas[1].Conf)
    // test TruncateBefore()
    err = manager.TruncateBefore(nextIndex - 1)
    assert.Nil(t, err)
    meta, err = manager.PreviousOf(nextIndex)
    assert.NotNil(t, err)
    metas, err = manager.ListAfter(nextIndex)
    assert.Nil(t, err)
    assert.Equal(t, 1, len(metas))
    assert.Equal(t, conf2, metas[0].Conf)
    // test TruncateAfter()
    c, err = manager.TruncateAfter(nextIndex)
    assert.Nil(t, err)
    assert.Equal(t, conf2, c)
    metas, err = manager.ListAfter(firstIndex)
    assert.NotNil(t, err)
}
