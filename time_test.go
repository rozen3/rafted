package rafted

import (
    "github.com/hhkbp2/testify/assert"
    "testing"
    "time"
)

func TestRandomLessDuration(t *testing.T) {
    timeout := HeartbeatTimeout
    var maxJitter float32 = 0.1
    rtimeout := RandomLessDuration(timeout, maxJitter)
    assert.True(t, timeout > rtimeout)
    timeoutLowerBound := time.Duration(int64(float64(int64(timeout)) * (1 - 0.1)))
    assert.True(t, rtimeout >= timeoutLowerBound)
}

func TestSimpleTicker(t *testing.T) {
    timeout := time.Millisecond * 50
    totalTime := time.Second * 1
    ticker := NewSimpleTicker(timeout)
    timeoutCount := 0
    onTimeout := func() {
        timeoutCount++
    }
    // test timeout callback
    ticker.Start(onTimeout)
    // sleep 1 more millisecond to ensure that
    // the callback in ticker is triggered for the last time.
    time.Sleep(totalTime + time.Millisecond + 1)
    assert.Equal(t, totalTime/timeout, timeoutCount)
    // test Stop()
    ticker.Stop()
    time.Sleep(totalTime)
    assert.Equal(t, totalTime/timeout, timeoutCount)
    // test Reset()
    timeoutCount = 0
    ticker.Start(onTimeout)
    time.Sleep(time.Millisecond * 40)
    ticker.Reset()
    time.Sleep(time.Millisecond * 40)
    assert.Equal(t, 0, timeoutCount)
    ticker.Stop()
}

func TestRandomTicker(t *testing.T) {
    timeout := time.Millisecond * 50
    totalTime := time.Second * 1
    maxJitter := float32(0.2)
    ticker := NewRandomTicker(timeout, maxJitter)
    timeoutCount := 0
    onTimeout := func() {
        timeoutCount++
    }
    // test timeout callback
    ticker.Start(onTimeout)
    time.Sleep(totalTime)
    assert.True(t, int64(totalTime/timeout) <= int64(timeoutCount))
    // test Stop()
    ticker.Stop()
    time.Sleep(totalTime)
    assert.True(t, int64(totalTime/timeout) <= int64(timeoutCount))
    // test Reset()
    timeoutCount = 0
    ticker.Start(onTimeout)
    time.Sleep(time.Millisecond * 40)
    ticker.Reset()
    time.Sleep(time.Millisecond * 40)
    assert.Equal(t, 0, timeoutCount)
    ticker.Stop()
}
