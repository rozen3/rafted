package rafted

import (
    "github.com/hhkpb2/rafted/persist"
    "net"
)

type TCPServerAddr struct {
    *net.TCPAddr
}

func (self *TCPServerAddr) Encode() ([]byte, error) {
    return []byte(addr.String()), nil
}

func (self *TCPServerAddr) Decode([]byte) error {
    return net.ResolveTCPAddr("", string(addr))
}

func EncodeServerAddrs(addrs []persist.ServerAddr) ([][]byte, error) {
    if addrs == nil {
        return nil, nil
    }
    var result bytes.Buffer
    for _, addr := range addrs {
        addrBin, err := add.Encode()
        if err != nil {
            return nil, errors.New(
                fmt.Sprintf("fail to encode addr: %s", addr.String()))
        }
        if _, err := result.Write(addrBin); err != nil {
            return nil, err
        }
    }
    return result.Bytes(), nil
}

func EncodeConfig(conf *persist.Config) (*persist.Configuration, error) {
    if conf == nil {
        return nil, nil
    }
    ServersBin, err := EncodeServerAddrs(conf.Servers)
    if err != nil {
        return nil, err
    }
    NewServersBin, err := EncodeServerAddrs(conf.NewServers)
    if err != nil {
        return nil, err
    }
    conf := &persist.Configuration{
        Servers:    ServersBin,
        NewServers: NewServersBin,
    }
    return conf, nil
}

func DecodeServerAddrs(bin [][]byte) ([]persist.ServerAddr, error) {
    // TODO add impl
    return make([]persist.ServerAddr, 0), nil
}

func DecodeConfig(conf *persist.Configuration) (*persist.Config, error) {
    // TODO add impl
    return &persist.Config{}, nil
}

func IsNormalConfig(conf *persist.Config) bool {
    return (conf.Servers != nil) && (conf.NewServers == nil)
}

func IsOldNewConfig(conf *persist.Config) bool {
    return (conf.Servers != nil) && (conf.NewServers != nil)
}

func IsNewConfig(conf *persist.Config) bool {
    return (conf.Server == nil) && (conf.NewServers != nil)
}
