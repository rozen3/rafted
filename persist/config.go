package persist

import (
    "net"
)

type ServerAddr interface {
    net.Addr
    Encode() ([]byte, error)
    Decode([]byte) error
}

// Config is the in-memory representation of membership of the cluster.
// Configuration is the serialization representation of Config.
type Config struct {
    Servers    []ServerAddr
    NewServers []ServerAddr
}

// ConfigMeta is metadata for a Config.
type ConfigMeta struct {
    // the index of log entry at which this config starts to take effect.
    FromLogIndex uint64
    // the index of log entry right after which
    // this config no long takes effect.
    ToLogIndex uint64
    // the config, which is effective in [FromLogIndex, ToLogIndex]
    Conf *Config
}

// ConfigManager is the interface for durable config management.
// It provides functions to store and restrieve config.
type ConfigManager interface {
    // Store a new config at the log entry with specified index
    PushConfig(logIndex uint64, conf *Config) error

    // Returns the last config
    LastConfig() (*Config, error)

    // Returns the config at log entry with specified index
    GetConfig(logIndex uint64) (*Config, error)

    // Lists all durable config metadatas after the given index,
    // including the given index.
    ListAfter(logIndex uint64) ([]*ConfigMeta, error)

    // Lists all durable config metadatas.
    // Metadatas should be returned in ascending log index order,
    // with smallest index first.
    List() ([]*ConfigMeta, error)

    // Delete the config metadata before and up to the given index,
    // including the given index.
    TruncateBefore(logIndex uint64) error

    // Delete the config metadata after the given index,
    // including the given index. And returns the config
    // at the log entry right before the given index.
    TruncateAfter(logIndex uint64) (*Config, error)
}
