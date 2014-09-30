package persist

import (
    "bytes"
    "encoding/binary"
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
