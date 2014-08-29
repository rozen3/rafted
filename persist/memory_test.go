package persist

import (
    "github.com/hhkbp2/rafted/str"
    "github.com/stretchr/testify/assert"
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
    e, err := log.GetLog(index)
    assert.Nil(t, err)
    assert.Equal(t, entry, e)
    entries, err = log.GetLogInRange(index, index)
    assert.Nil(t, err)
    assert.Equal(t, 1, len(entries))
    assert.Equal(t, entry, entries[0])
    // test CommittedIndex, LastAppliedIndex
    err = log.StoreCommittedIndex(23356)
    assert.NotNil(t, err)
    assert.Equal(t, 0, ValueOf(log.CommittedIndex()))
    committedIndex := uint64(20000)
    err = log.StoreCommittedIndex(committedIndex)
    assert.Nil(t, err)
    assert.Equal(t, committedIndex, ValueOf(log.CommittedIndex()))
}
