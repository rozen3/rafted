package rafted

import "time"
import "math/rand"
import "sync"

func RandomDuration(value time.Duration) time.Duration {
    return RandomMultipleDuration(value, 2)
}

func RandomMultipleDuration(value time.Duration, maxMultiple uint32) time.Duration {
    return time.Duration(rand.Int63()) % MultipleDuration(value, maxMultiple)
}

func MultipleDuration(value time.Duration, multiple uint32) time.Duration {
    sum := value

    for i := uint32(0); i < multiple; i++ {
        sum += value
    }
    return sum
}

func TimeExpire(lastTime time.Time, timeout time.Duration) bool {
    if time.Now().Sub(lastTime) < timeout {
        return false
    }
    return true
}

type Ticker interface {
    Start(fn func())
    Stop()
}

type SimpleTicker struct {
    ticker   *time.Ticker
    stopChan chan interface{}
    group    *sync.WaitGroup
}

func NewSimpleTicker(timeout time.Duration) *SimpleTicker {
    return &SimpleTicker{
        time.NewTicker(timeout),
        make(chan interface{}),
        &sync.WaitGroup{},
    }
}

func (self *SimpleTicker) Start(fn func()) {
    self.group.Add(1)
    go self.start(fn)
}

func (self *SimpleTicker) start(fn func()) {
    defer self.group.Done()
    for {
        select {
        case <-self.stopChan:
            return
        case <-self.ticker.C:
            fn()
        }
    }
}

func (self *SimpleTicker) Stop() {
    self.stopChan <- self
    self.group.Wait()
}

type RandomTicker struct {
    timeout  time.Duration
    stopChan chan interface{}
    group    *sync.WaitGroup
}

func NewRandomTicker(timeout time.Duration) *RandomTicker {
    return &RandomTicker{
        timeout,
        make(chan interface{}),
        &sync.WaitGroup{},
    }
}

func (self *RandomTicker) Start(fn func()) {
    self.group.Add(1)
    go self.start(fn)
}

func (self *RandomTicker) start(fn func()) {
    defer self.group.Done()
    for {
        timeChan := time.After(RandomDuration(self.timeout))
        select {
        case <-self.stopChan:
            return
        case <-timeChan:
            fn()
        }
    }
}

func (self *RandomTicker) Stop() {
    self.stopChan <- self
    self.group.Wait()
}
