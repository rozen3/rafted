package persist

import (
    "errors"
    "fmt"
    "github.com/hhkbp2/rafted/str"
    "math/rand"
    "net"
    "reflect"
    "strings"
    "time"
)

var (
    ErrorMultiAddrNoAddr error = errors.New("no Addr in MultiAddr")
)

var (
    MultiAddrStringSeperator string = ","
)

var (
    ServerAddressNil *ServerAddress = nil
)

func init() {
    rand.Seed(time.Now().UTC().UnixNano())
}

type Addr interface {
    net.Addr
    ISP() string
}

// Address represents single ip address.
type Address struct {
    Isp      string
    Protocol string
    IP       string
    Port     uint16
}

func (self *Address) Network() string {
    return self.Protocol
}

func (self *Address) String() string {
    return fmt.Sprintf("%s:%d", self.IP, self.Port)
}

func (self *Address) ISP() string {
    return self.Isp
}

func IsNil(i interface{}) bool {
    return (i == nil || reflect.ValueOf(i).IsNil())
}

func AddrEqual(addr1 Addr, addr2 Addr) bool {
    if IsNil(addr1) && IsNil(addr2) {
        return true
    } else if IsNil(addr1) || IsNil(addr2) {
        return false
    }
    return ((addr1.Network() == addr2.Network()) &&
        (addr1.String() == addr2.String()) &&
        (addr1.ISP() == addr2.ISP()))
}

func AddrNotEqual(addr1 Addr, addr2 Addr) bool {
    return !AddrEqual(addr1, addr2)
}

type MultiAddr interface {
    AllAddr() []Addr
    Len() int
    String() string
}

func FirstAddr(multiAddr MultiAddr) (Addr, error) {
    addrs := multiAddr.AllAddr()
    if (addrs == nil) || (len(addrs) == 0) {
        return nil, ErrorMultiAddrNoAddr
    }
    return addrs[0], nil
}

// ServerAddress represents the network address of any node in the cluster.
type ServerAddress struct {
    Addresses []*Address
}

func (self *ServerAddress) AllAddr() []Addr {
    length := len(self.Addresses)
    result := make([]Addr, 0, length)
    for i := 0; i < length; i++ {
        result = append(result, self.Addresses[i])
    }
    return result
}

func (self *ServerAddress) Len() int {
    return len(self.Addresses)
}

func (self *ServerAddress) String() string {
    length := len(self.Addresses)
    addrsInString := make([]string, 0, length)
    for _, addr := range self.Addresses {
        addrsInString = append(addrsInString, addr.String())
    }
    return strings.Join(addrsInString, MultiAddrStringSeperator)
}

func MultiAddrEqual(addr1 MultiAddr, addr2 MultiAddr) bool {
    if IsNil(addr1) && IsNil(addr2) {
        return true
    } else if IsNil(addr1) || IsNil(addr2) {
        return false
    }
    addrs1 := addr1.AllAddr()
    addrs2 := addr2.AllAddr()
    if len(addrs1) != len(addrs2) {
        return false
    }
    for i, addr := range addrs1 {
        if AddrNotEqual(addr, addrs2[i]) {
            return false
        }
    }
    return true
}

func MultiAddrNotEqual(addr1 MultiAddr, addr2 MultiAddr) bool {
    return !MultiAddrEqual(addr1, addr2)
}

type MultiAddrSlice interface {
    AllMultiAddr() []MultiAddr
    Len() int
}

func Len(slice MultiAddrSlice) int {
    if IsNil(slice) {
        return 0
    }
    return slice.Len()
}

type ServerAddressSlice struct {
    Addresses []*ServerAddress
}

func (self *ServerAddressSlice) AllMultiAddr() []MultiAddr {
    length := len(self.Addresses)
    result := make([]MultiAddr, 0, length)
    for i := 0; i < length; i++ {
        result = append(result, self.Addresses[i])
    }
    return result
}

func (self *ServerAddressSlice) Len() int {
    return len(self.Addresses)
}

func MultiAddrSliceEqual(slice1 MultiAddrSlice, slice2 MultiAddrSlice) bool {
    if IsNil(slice1) && IsNil(slice2) {
        return true
    } else if IsNil(slice1) || IsNil(slice2) {
        return false
    }

    addrs1 := slice1.AllMultiAddr()
    addrs2 := slice2.AllMultiAddr()
    if len(addrs1) != len(addrs2) {
        return false
    }
    for i, addr := range addrs1 {
        if MultiAddrNotEqual(addr, addrs2[i]) {
            return false
        }
    }
    return true
}

func MultiAddrSliceNotEqual(slice1 MultiAddrSlice, slice2 MultiAddrSlice) bool {
    return !MultiAddrSliceEqual(slice1, slice2)
}

func SetupSocketMultiAddrSlice(number int) *ServerAddressSlice {
    addrs := make([]*ServerAddress, 0, number)
    for i := 0; i < number; i++ {
        addr := &ServerAddress{
            Addresses: []*Address{
                &Address{
                    Protocol: "tcp",
                    IP:       "127.0.0.1",
                    Port:     uint16(6152 + i),
                },
            },
        }
        addrs = append(addrs, addr)
    }
    return &ServerAddressSlice{
        Addresses: addrs,
    }
}

func SetupMemoryMultiAddrSlice(number int) *ServerAddressSlice {
    addrs := make([]*ServerAddress, 0, number)
    for i := 0; i < number; i++ {
        addr := &ServerAddress{
            Addresses: []*Address{
                &Address{
                    Protocol: "memory",
                    IP:       "127.0.0.1",
                    Port:     uint16(6152 + i),
                },
            },
        }
        addrs = append(addrs, addr)
    }
    return &ServerAddressSlice{
        Addresses: addrs,
    }
}

func RandomMemoryMultiAddr() *ServerAddress {
    addr := &ServerAddress{
        Addresses: []*Address{
            &Address{
                Protocol: "memory",
                IP:       str.RandomIP(),
                Port:     uint16(rand.Intn(65536)),
            },
        },
    }
    return addr
}

func RandomMemoryMultiAddrSlice(number int) *ServerAddressSlice {
    addrs := make([]*ServerAddress, 0, number)
    for i := 0; i < number; i++ {
        addrs = append(addrs, RandomMemoryMultiAddr())
    }
    return &ServerAddressSlice{
        Addresses: addrs,
    }
}
