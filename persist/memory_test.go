package persist

import (
    "github.com/hhkbp2/rafted/str"
    "github.com/hhkbp2/testify/assert"
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
            Servers:    SetupMemoryMultiAddrSlice(5),
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
    conf := &Config{
        Servers:    SetupMemoryMultiAddrSlice(5),
        NewServers: nil,
    }
    id, err := stateMachine.MakeSnapshot(term, index, conf)
    assert.Nil(t, err)
    // test OpenSnapshot()
    meta, reader, err := stateMachine.OpenSnapshot(id)
    assert.Nil(t, err)
    assert.Equal(t, term, meta.LastIncludedTerm)
    assert.Equal(t, index, meta.LastIncludedIndex)
    assert.Equal(t, conf, meta.Conf)
    assert.Equal(t, 2, meta.Size)
    // test LastSnapshotInfo()
    metaOfLastSnapshot, err := stateMachine.LastSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, meta, metaOfLastSnapshot)
    // test AllSnapshotInfo()
    allMetas, err := stateMachine.AllSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, 1, len(allMetas))
    assert.Equal(t, meta, allMetas[0])
    // test RestoreFromSnapshot()
    data3 := []byte(str.RandomString(100))
    stateMachine.Apply(data3)
    nextTerm := term + 1
    nextIndex := index + 1
    id2, err := stateMachine.MakeSnapshot(nextTerm, nextIndex, conf)
    assert.Nil(t, err)
    meta2, reader, err := stateMachine.OpenSnapshot(id2)
    assert.Nil(t, err)
    assert.Equal(t, nextTerm, meta2.LastIncludedTerm)
    assert.Equal(t, nextIndex, meta2.LastIncludedIndex)
    assert.Equal(t, conf, meta2.Conf)
    assert.Equal(t, 3, meta2.Size)
    err = stateMachine.RestoreFromSnapshot(id)
    assert.Nil(t, err)
    id3, err := stateMachine.MakeSnapshot(
        meta.LastIncludedTerm, meta.LastIncludedIndex, meta.Conf)
    assert.Nil(t, err)
    meta3, reader, err := stateMachine.OpenSnapshot(id3)
    assert.Equal(t, meta, meta3)
    // test DeleteSnapshot()
    allMetas, err = stateMachine.AllSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, 3, len(allMetas))
    err = stateMachine.DeleteSnapshot(id3)
    assert.Nil(t, err)
    allMetas, err = stateMachine.AllSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, 2, len(allMetas))
    // test MakeEmptySnapshot()
    writer, err := stateMachine.MakeEmptySnapshot(term, index, conf)
    assert.Nil(t, err)
    n, err := writer.Write(data1)
    assert.Nil(t, err)
    assert.Equal(t, len(data1), n)
    n, err = writer.Write(data2)
    assert.Nil(t, err)
    assert.Equal(t, len(data2), n)
    id4 := writer.ID()
    err = writer.Close()
    assert.Nil(t, err)
    meta, err = stateMachine.LastSnapshotInfo()
    assert.Nil(t, err)
    assert.Equal(t, meta.ID, id4)
    meta, reader, err = stateMachine.OpenSnapshot(id4)
    assert.Nil(t, err)
    assert.Equal(t, term, meta.LastIncludedTerm)
    assert.Equal(t, index, meta.LastIncludedIndex)
    assert.Equal(t, conf, meta.Conf)
    dataLen := len(data1) + len(data2)
    p := make([]byte, dataLen+1)
    n, err = reader.Read(p)
    assert.Nil(t, err)
    assert.Equal(t, dataLen, n)
}

func TestMemoryConfigManager(t *testing.T) {
    firstIndex := uint64(333)
    conf := &Config{
        Servers:    SetupMemoryMultiAddrSlice(5),
        NewServers: SetupMemoryMultiAddrSlice(5),
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
