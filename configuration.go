package rafted

import (
    cm "github.com/hhkbp2/rafted/comm"
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
    CommClientTimeout               time.Duration
    CommServerTimeout               time.Duration
    CommPoolSize                    int
    ClientTimeout                   time.Duration
    RPCServerAuth                   *cm.RPCAuth
    RPCClientAuth                   *cm.RPCAuth
}

func DefaultConfiguration() *Configuration {
    auth := &cm.RPCAuth{
        User:     "user",
        Password: "password",
    }
    return &Configuration{
        HeartbeatTimeout:                time.Millisecond * 50,
        ElectionTimeout:                 time.Millisecond * 200,
        ElectionTimeoutThresholdPersent: float64(0.8),
        MaxTimeoutJitter:                float32(0.1),
        PersistErrorNotifyTimeout:       time.Millisecond * 100,
        MaxAppendEntriesSize:            uint64(10),
        MaxSnapshotChunkSize:            uint64(1000),
        CommClientTimeout:               time.Millisecond * 500,
        CommServerTimeout:               time.Minute * 30,
        CommPoolSize:                    10,
        ClientTimeout:                   time.Millisecond * 100,
        RPCServerAuth:                   auth,
        RPCClientAuth:                   auth,
    }
}
