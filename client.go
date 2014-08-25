package rafted

import (
    //    cm "github.com/hhkbp2/rafted/comm"
    ps "github.com/hhkbp2/rafted/persist"
)

type Client interface {
    Append(data []byte) (result []byte, err error)
    ReadOnly(data []byte) (result []byte, err error)
    GetConfig() (servers []ps.ServerAddr, err error)
    ChangeConfig(oldServers []ps.ServerAddr, newServers []ps.ServerAddr) error
}

type RedirectClient struct {
}

func (self *RedirectClient) Append(data []byte) (result []byte, err error) {
    // TODO add impl
    return nil, nil
}

func (self *RedirectClient) ReadOnly(data []byte) (result []byte, err error) {
    // TODO add impl
    return nil, nil
}

func (self *RedirectClient) GetConfig() (servers []ps.ServerAddr, err error) {
    // TODO add impl
    return nil, nil
}

func (self *RedirectClient) ChangeConfig(
    oldServers []ps.ServerAddr, newServers []ps.ServerAddr) error {

    // TODO add impl
    return nil
}
