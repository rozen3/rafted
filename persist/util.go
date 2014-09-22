package persist

import (
    "bytes"
    "container/list"
    "fmt"
    "github.com/hhkbp2/rafted/str"
    "math/rand"
    "time"
)

var NilServerAddr ServerAddr = ServerAddr{}

func Timestamp() string {
    return time.Now().Format("2006-01-02 15:04:05")
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

func (self *ServerAddr) Network() string {
    return self.Protocol
}

func (self *ServerAddr) String() string {
    if len(self.Protocol) == 0 {
        return fmt.Sprintf("%s:%d", self.IP, self.Port)
    }
    return fmt.Sprintf("%s:%d", self.IP, self.Port)
}

func AddrEqual(addr1 *ServerAddr, addr2 *ServerAddr) bool {
    if (addr1 == nil) && (addr2 == nil) {
        return true
    } else if (addr1 == nil) || (addr2 == nil) {
        return false
    }
    return ((addr1.Protocol == addr2.Protocol) &&
        (addr1.IP == addr2.IP) &&
        (addr1.Port == addr2.Port))
}

func AddrNotEqual(addr1 *ServerAddr, addr2 *ServerAddr) bool {
    return !AddrEqual(addr1, addr2)
}

func AddrsEqual(addrs1 []ServerAddr, addrs2 []ServerAddr) bool {
    if (addrs1 == nil) && (addrs2 == nil) {
        return true
    } else if (addrs1 == nil) || (addrs2 == nil) {
        return false
    }
    if len(addrs1) != len(addrs2) {
        return false
    }
    for i, addr := range addrs1 {
        if AddrNotEqual(&addr, &addrs2[i]) {
            return false
        }
    }
    return true
}

func AddrsNotEqual(addrs1 []ServerAddr, addrs2 []ServerAddr) bool {
    return !AddrsEqual(addrs1, addrs2)
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

func ConfigNotEqual(conf1 *Config, conf2 *Config) bool {
    return !ConfigEqual(conf1, conf2)
}

func CopyConfig(conf *Config) *Config {
    return &Config{
        Servers:    conf.Servers[:],
        NewServers: conf.NewServers[:],
    }
}

func SetupSocketServerAddrs(number int) []ServerAddr {
    addrs := make([]ServerAddr, 0, number)
    for i := 0; i < number; i++ {
        addr := ServerAddr{
            Protocol: "tcp",
            IP:       "127.0.0.1",
            Port:     uint16(6152 + i),
        }
        addrs = append(addrs, addr)
    }
    return addrs
}

func SetupMemoryServerAddrs(number int) []ServerAddr {
    addrs := make([]ServerAddr, 0, number)
    for i := 0; i < number; i++ {
        addr := ServerAddr{
            Protocol: "memory",
            IP:       "127.0.0.1",
            Port:     uint16(6152 + i),
        }
        addrs = append(addrs, addr)
    }
    return addrs
}

func RandomMemoryServerAddr() ServerAddr {
    rand.Seed(time.Now().UTC().UnixNano())
    addr := ServerAddr{
        Protocol: "memory",
        IP:       str.RandomIP(),
        Port:     uint16(rand.Intn(65536)),
    }
    return addr
}

func RandomMemoryServerAddrs(number int) []ServerAddr {
    addrs := make([]ServerAddr, 0, number)
    for i := 0; i < number; i++ {
        addrs = append(addrs, RandomMemoryServerAddr())
    }
    return addrs
}

func LogEntryEqual(entry1 *LogEntry, entry2 *LogEntry) bool {
    return (entry1.Term == entry2.Term) &&
        (entry1.Index == entry2.Index) &&
        (entry1.Type == entry2.Type) &&
        (entry1.Type == entry2.Type) &&
        (bytes.Compare(entry1.Data, entry2.Data) == 0) &&
        ConfigEqual(entry1.Conf, entry2.Conf)
}

func IsInMemeberChange(conf *Config) bool {
    return (conf.NewServers != nil)
}

func IsNormalConfig(conf *Config) bool {
    return (conf.Servers != nil) && (conf.NewServers == nil)
}

func IsOldNewConfig(conf *Config) bool {
    return (conf.Servers != nil) && (conf.NewServers != nil)
}

func IsNewConfig(conf *Config) bool {
    return (conf.Servers == nil) && (conf.NewServers != nil)
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
