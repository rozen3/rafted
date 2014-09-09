package persist

import (
    "bytes"
    "encoding/binary"
    "github.com/deckarep/golang-set"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestBinaryReadWrite(t *testing.T) {
    i := int32(100)
    p := make([]byte, 0, 100)
    buf := bytes.NewBuffer(p)
    err := binary.Write(buf, binary.BigEndian, &i)
    assert.Nil(t, err)
    v := int32(0)
    err = binary.Read(buf, binary.BigEndian, &v)
    assert.Nil(t, err)
    assert.Equal(t, i, v)
}

func TestAddrEqual(t *testing.T) {
    addr1 := RandomMemoryServerAddr()
    addr2 := RandomMemoryServerAddr()
    assert.False(t, AddrEqual(nil, &addr1))
    assert.False(t, AddrEqual(&addr1, nil))
    assert.True(t, AddrEqual(nil, nil))
    assert.True(t, AddrEqual(&addr1, &addr1))
    assert.True(t, AddrEqual(&addr2, &addr2))
    assert.False(t, AddrEqual(&addr2, &addr1))
}

func TestAddrNotEqual(t *testing.T) {
    addr1 := RandomMemoryServerAddr()
    addr2 := RandomMemoryServerAddr()
    assert.True(t, AddrNotEqual(nil, &addr1))
    assert.True(t, AddrNotEqual(&addr1, nil))
    assert.False(t, AddrNotEqual(nil, nil))
    assert.False(t, AddrNotEqual(&addr1, &addr1))
    assert.False(t, AddrNotEqual(&addr2, &addr2))
    assert.True(t, AddrNotEqual(&addr2, &addr1))
}

func TestAddrsEqual(t *testing.T) {
    size := 10
    addrs1 := RandomMemoryServerAddrs(size)
    addrs2 := RandomMemoryServerAddrs(size)
    assert.False(t, AddrsEqual(nil, addrs1))
    assert.False(t, AddrsEqual(addrs1, nil))
    assert.True(t, AddrsEqual(nil, nil))
    assert.True(t, AddrsEqual(addrs1, addrs1))
    assert.True(t, AddrsEqual(addrs2, addrs2))
    assert.False(t, AddrsEqual(addrs1, addrs2))
}

func TestAddrsNotEqual(t *testing.T) {
    size := 20
    addrs1 := RandomMemoryServerAddrs(size)
    addrs2 := RandomMemoryServerAddrs(size)
    assert.True(t, AddrsNotEqual(nil, addrs1))
    assert.True(t, AddrsNotEqual(addrs1, nil))
    assert.False(t, AddrsNotEqual(nil, nil))
    assert.False(t, AddrsNotEqual(addrs1, addrs1))
    assert.False(t, AddrsNotEqual(addrs2, addrs2))
    assert.True(t, AddrsNotEqual(addrs1, addrs2))
}

func TestSetupMemoryServerAddrs(t *testing.T) {
    i := 50
    addrs := SetupMemoryServerAddrs(i)
    assert.Equal(t, i, len(addrs))
    m := mapset.NewThreadUnsafeSet()
    for _, addr := range addrs {
        m.Add(addr)
    }
    assert.Equal(t, i, m.Cardinality())
}

func TestRandomMemoryServerAddr(t *testing.T) {
    addr1 := RandomMemoryServerAddr()
    addr2 := RandomMemoryServerAddr()
    assert.NotEqual(t, addr1.IP, addr2.IP)
    assert.NotEqual(t, addr1.Port, addr2.Port)
}

func TestRandomMemoryServerAddrs(t *testing.T) {
    i := 19
    addrs := RandomMemoryServerAddrs(i)
    assert.Equal(t, i, len(addrs))
    // test no repetition
    m := mapset.NewThreadUnsafeSet()
    for _, addr := range addrs {
        m.Add(addr)
    }
    assert.Equal(t, i, m.Cardinality())
}
