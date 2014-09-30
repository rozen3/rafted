package persist

import (
    "github.com/deckarep/golang-set"
    "github.com/hhkbp2/testify/assert"
    "testing"
)

func TestMultiAddrEqual(t *testing.T) {
    addr1 := RandomMemoryMultiAddr()
    addr2 := RandomMemoryMultiAddr()
    assert.False(t, MultiAddrEqual(nil, addr1))
    assert.False(t, MultiAddrEqual(addr1, nil))
    assert.True(t, MultiAddrEqual(nil, nil))
    assert.True(t, MultiAddrEqual(addr1, addr1))
    assert.True(t, MultiAddrEqual(addr2, addr2))
    assert.False(t, MultiAddrEqual(addr2, addr1))
}

func TestMultiAddrNotEqual(t *testing.T) {
    addr1 := RandomMemoryMultiAddr()
    addr2 := RandomMemoryMultiAddr()
    assert.True(t, MultiAddrNotEqual(nil, addr1))
    assert.True(t, MultiAddrNotEqual(addr1, nil))
    assert.False(t, MultiAddrNotEqual(nil, nil))
    assert.False(t, MultiAddrNotEqual(addr1, addr1))
    assert.False(t, MultiAddrNotEqual(addr2, addr2))
    assert.True(t, MultiAddrNotEqual(addr2, addr1))
}

func TestMultiAddrSliceEqual(t *testing.T) {
    size := 10
    slice1 := RandomMemoryMultiAddrSlice(size)
    slice2 := RandomMemoryMultiAddrSlice(size)
    assert.False(t, MultiAddrSliceEqual(nil, slice1))
    assert.False(t, MultiAddrSliceEqual(slice1, nil))
    assert.True(t, MultiAddrSliceEqual(nil, nil))
    assert.True(t, MultiAddrSliceEqual(slice1, slice1))
    assert.True(t, MultiAddrSliceEqual(slice2, slice2))
    assert.False(t, MultiAddrSliceEqual(slice1, slice2))
}

func TestMultiAddrSliceEqualNotEqual(t *testing.T) {
    size := 20
    slice1 := RandomMemoryMultiAddrSlice(size)
    slice2 := RandomMemoryMultiAddrSlice(size)
    assert.True(t, MultiAddrSliceNotEqual(nil, slice1))
    assert.True(t, MultiAddrSliceNotEqual(slice1, nil))
    assert.False(t, MultiAddrSliceNotEqual(nil, nil))
    assert.False(t, MultiAddrSliceNotEqual(slice1, slice1))
    assert.False(t, MultiAddrSliceNotEqual(slice2, slice2))
    assert.True(t, MultiAddrSliceNotEqual(slice1, slice2))
}

func testSetupMultiAddrSlice(
    t *testing.T, genSlice func(size int) *ServerAddressSlice) {

    size := 50
    slice := genSlice(size)
    addrs := slice.AllMultiAddr()
    assert.Equal(t, size, len(addrs))
    // check no repetition
    m := mapset.NewThreadUnsafeSet()
    for _, addr := range addrs {
        m.Add(addr)
    }
    assert.Equal(t, size, m.Cardinality())
}

func TestSetupSocketMultiAddrSlice(t *testing.T) {
    testSetupMultiAddrSlice(t, SetupSocketMultiAddrSlice)
}

func TestSetupMemoryMultiAddrSlice(t *testing.T) {
    testSetupMultiAddrSlice(t, SetupMemoryMultiAddrSlice)
}

func TestRandomMemoryMultiAddr(t *testing.T) {
    addr1 := RandomMemoryMultiAddr()
    addr2 := RandomMemoryMultiAddr()
    assert.True(t, MultiAddrNotEqual(addr1, addr2))
}

func TestRandomMemoryMultiAddrSlice(t *testing.T) {
    testSetupMultiAddrSlice(t, RandomMemoryMultiAddrSlice)
}
