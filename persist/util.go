package persist

import (
    "bytes"
    "encoding/binary"
    "io"
    "time"
)

func Timestamp() string {
    return time.Now().Format("2006-01-02 15:04:05")
}

func Uint64ToBytes(i uint64) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, i)
    return buf
}

func BytesToUint64(buf []byte) uint64 {
    return uint64(binary.BigEndian.Uint64(buf))
}

type ByteReaderWrapper struct {
    io.Reader
}

func NewByteReaderWrapper(reader io.Reader) *ByteReaderWrapper {
    return &ByteReaderWrapper{
        reader,
    }
}

func (self *ByteReaderWrapper) ReadByte() (byte, error) {
    p := make([]byte, 1)
    if _, err := self.Read(p); err != nil {
        return p[0], err
    }
    return p[0], nil
}

type ReaderCloserWrapper struct {
    *bytes.Reader
}

func NewReaderCloserWrapper(reader *bytes.Reader) *ReaderCloserWrapper {
    return &ReaderCloserWrapper{
        Reader: reader,
    }
}

func (self *ReaderCloserWrapper) Close() error {
    // empty body
    return nil
}
