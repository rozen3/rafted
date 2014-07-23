package comm

import (
    crand "crypto/rand"
    "fmt"
)

func GenerateRandomUUID() string {
    buf := make([]byte, 16)
    if _, err := crand.Read(buf); err != nil {
        panic(fmt.Errorf("failed to read random bytes: %v", err))
    }

    return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
        buf[0:4],
        buf[4:6],
        buf[6:8],
        buf[8:10],
        buf[10:16])
}
