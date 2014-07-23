package rafted

import (
    "net"
)

func EncodeAddr(addr net.Addr) ([]byte, error) {
    if addr == nil {
        return nil, nil
    }
    return []byte(addr.String()), nil
}

func DecodeAddr(addr []byte) (net.Addr, error) {
    return net.ResolveTCPAddr("", string(addr))
}

// min returns the minimum.
func min(a, b uint64) uint64 {
    if a <= b {
        return a
    }
    return b
}

// max returns the maximum
func max(a, b uint64) uint64 {
    if a >= b {
        return a
    }
    return b
}
