package persist

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
    Push(logIndex uint64, conf *Config) error

    // Returns the nth config(start from 0) from back to front.
    RNth(n uint32) (*Config, error)

    // Returns the previous one of specified config
    PreviousOf(logIndex uint64) (*ConfigMeta, error)

    // Lists all durable config metadatas after the given index,
    // including the given index.
    // Metadatas should be returned in ascending log index order,
    // with smallest index first.
    ListAfter(logIndex uint64) ([]*ConfigMeta, error)

    // Delete the config metadata before and up to the given index,
    // including the given index.
    TruncateBefore(logIndex uint64) error

    // Delete the config metadata after the given index,
    // including the given index. And returns the config
    // at the log entry right before the given index.
    TruncateAfter(logIndex uint64) (*Config, error)
}
