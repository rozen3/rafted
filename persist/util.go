package persist

import (
    "bytes"
    "container/list"
    "encoding/binary"
    "io"
    "net"
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

func AddrEqual(addr1 net.Addr, addr2 net.Addr) bool {
    if (addr1 == nil) && (addr2 == nil) {
        return true
    } else if (addr1 == nil) || (addr2 == nil) {
        return false
    }
    return ((addr1.Network() == addr2.Network()) &&
        (addr1.String() == addr2.String()))
}

func AddrsEqual(addrs1 []net.Addr, addrs2 []net.Addr) bool {
    if len(addrs1) != len(addrs2) {
        return false
    }
    for i, addr := range addrs1 {
        if !AddrEqual(addr, addrs2[i]) {
            return false
        }
    }
    return true
}

func ConfigEqual(conf1 *Config, conf2 *Config) bool {
    if (conf1.Servers == nil) && (conf2.Servers == nil) {
        if (conf1.NewServers == nil) && (conf2.NewServers == nil) {
            return true
        } else if (conf1.NewServers == nil) || (conf2.NewServers == nil) {
            return false
        }
        return AddrsEqual(conf1.NewServers, conf2.NewServers)
    } else if (conf1.Servers == nil) || (conf1.Servers == nil) {
        return false
    }

    if (conf1.NewServers == nil) && (conf2.NewServers == nil) {
        return AddrsEqual(conf1.Servers, conf2.Servers)
    } else if (conf1.NewServers == nil) || (conf2.NewServers == nil) {
        return false
    }
    return (AddrsEqual(conf1.Servers, conf2.Servers) &&
        AddrsEqual(conf1.NewServers, conf2.NewServers))
}

func CopyConfig(conf *Config) *Config {
    return &Config{
        Servers:    conf.Servers[:],
        NewServers: conf.NewServers[:],
    }
}

func IsInMemeberChange(conf *Config) bool {
    return (conf.NewServers != nil)
}

// ListTruncate() removes elements from `e' to the last element in list `l'.
// The range to be removed is [e, l.Back()]. It returns list `l'.
func ListTruncate(l *list.List, e *list.Element) *list.List {
    // remove `e' and all elements after `e'
    var next *list.Element
    for ; e != nil; e = next {
        next = e.Next()
        l.Remove(e)
    }
    return l
}

func ListTruncateHead(l *list.List, e *list.Element) *list.List {
    for elem := l.Front(); elem != e; elem = elem.Next() {
        l.Remove(e)
    }
    return l
}
