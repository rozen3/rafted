package rafted

import (
    "time"
)

type Configuration struct {
    HeartbeatTimeout                time.Duration
    ElectionTimeout                 time.Duration
    ElectionTimeoutThresholdPersent float64
    MaxTimeoutJitter                float32
    PersistErrorNotifyTimeout       time.Duration
    MaxAppendEntriesSize            uint64
    MaxSnapshotChunkSize            uint64
    CommTimeout                     time.Duration
    CommPoolSize                    int
}

func DefaultConfiguration() *Configuration {
    return &Configuration{
        HeartbeatTimeout:                time.Millisecond * 50,
        ElectionTimeout:                 time.Millisecond * 200,
        ElectionTimeoutThresholdPersent: float64(0.8),
        MaxTimeoutJitter:                float32(0.1),
        PersistErrorNotifyTimeout:       time.Millisecond * 100,
        MaxAppendEntriesSize:            uint64(10),
        MaxSnapshotChunkSize:            uint64(1000),
        CommTimeout:                     time.Millisecond * 50,
        CommPoolSize:                    10,
    }
}
