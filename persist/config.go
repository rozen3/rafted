package persist

import (
    "net"
)

type Config struct {
    Servers    []net.Addr
    NewServers []net.Addr
}

type ConfigMeta struct {
    FromLogIndex uint64
    ToLogIndex   uint64
    Conf         *Config
}

type ConfigManager interface {
    PushConfig(logIndex uint64, conf *Config) error
    TruncateConfigAfter(logIndex uint64) (*Config, error)
    LastConfig() (*Config, error)
    GetConfig(logIndex uint64) (*Config, error)
    List() ([]*ConfigMeta, error)
}
