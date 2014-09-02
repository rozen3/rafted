package rafted

import (
    rt "github.com/hhkbp2/rafted/retry"
    "sync"
    "time"
)

func RandomLessDuration(d time.Duration, maxJitter float32) time.Duration {
    jitter := float64(rt.RandIntN(int(maxJitter*100))+1) / 100
    return time.Duration(int64(float64(int64(d)) * (1 - jitter)))
}

func TimeExpire(lastTime time.Time, timeout time.Duration) bool {
    if time.Now().Sub(lastTime) < timeout {
        return false
    }
    return true
}

type Ticker interface {
    Start(fn func())
    Reset()
    Stop()
}

type SimpleTicker struct {
    timeout   time.Duration
    stopChan  chan interface{}
    resetChan chan interface{}
    group     *sync.WaitGroup
}

func NewSimpleTicker(timeout time.Duration) *SimpleTicker {
    return &SimpleTicker{
        timeout:   timeout,
        stopChan:  make(chan interface{}),
        resetChan: make(chan interface{}),
        group:     &sync.WaitGroup{},
    }
}

func (self *SimpleTicker) Start(fn func()) {
    routine := func() {
        defer self.group.Done()
        ticker := time.NewTicker(self.timeout)
        for {
            select {
            case <-self.stopChan:
                return
            case <-self.resetChan:
                ticker = time.NewTicker(self.timeout)
            case <-ticker.C:
                fn()
            }
        }

    }
    self.group.Add(1)
    go routine()
}

func (self *SimpleTicker) Reset() {
    self.resetChan <- self
}

func (self *SimpleTicker) Stop() {
    self.stopChan <- self
    self.group.Wait()
}

type RandomTicker struct {
    timeout   time.Duration
    maxJitter float32
    stopChan  chan interface{}
    resetChan chan interface{}
    group     *sync.WaitGroup
}

func NewRandomTicker(timeout time.Duration, maxJitter float32) *RandomTicker {
    return &RandomTicker{
        timeout:   timeout,
        maxJitter: maxJitter,
        stopChan:  make(chan interface{}),
        resetChan: make(chan interface{}),
        group:     &sync.WaitGroup{},
    }
}

func (self *RandomTicker) Start(fn func()) {
    routine := func() {
        defer self.group.Done()
        for {
            timeChan := time.After(
                RandomLessDuration(self.timeout, self.maxJitter))
            select {
            case <-self.stopChan:
                return
            case <-self.resetChan:
            case <-timeChan:
                fn()
            }
        }
    }
    self.group.Add(1)
    go routine()
}

func (self *RandomTicker) Reset() {
    self.resetChan <- self
}

func (self *RandomTicker) Stop() {
    self.stopChan <- self
    self.group.Wait()
}
