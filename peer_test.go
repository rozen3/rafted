package rafted

import (
    "fmt"
    hsm "github.com/hhkbp2/go-hsm"
    ps "github.com/hhkbp2/rafted/persist"
    "github.com/hhkbp2/testify/mock"
    "testing"
)

type MockLocal struct {
    mock.Mock

    configManager   ps.ConfigManager
    log             ps.Log
    snapshotManager ps.SnapshotManager
    notifier        *Notifier
    Events          []hsm.Event
    PriorEvents     []hsm.Event
}

func NewMockLocal(configManager ps.ConfigManager) *MockLocal {
    return &MockLocal{
        Events:      make([]hsm.Event, 0),
        PriorEvents: make([]hsm.Event, 0),
    }
}

func (self *MockLocal) Send(event hsm.Event) {
    self.Events = append(self.Events, event)
    self.Mock.Called(event)
}

func (self *MockLocal) SendPrior(event hsm.Event) {
    self.PriorEvents = append(self.PriorEvents, event)
    self.Mock.Called(event)
}

func (self *MockLocal) Terminate() {
    self.Mock.Called()
}

func (self *MockLocal) QueryState() {
    self.Mock.Called()
}

func (self *MockLocal) GetCurrentTerm() uint64 {
    args := self.Mock.Called()
    return args.Uint64(0)
}

func (self *MockLocal) getAddr() ps.ServerAddr {
    args := self.Mock.Called()
    var s ps.ServerAddr
    var ok bool
    if s, ok = args.Get(0).(ps.ServerAddr); !ok {
        panic(fmt.Sprintf("argument: %s not correct type ServerAddr",
            args.Get(0)))
    }
    return s
}

func (self *MockLocal) GetLocalAddr() ps.ServerAddr {
    return self.getAddr()
}

func (self *MockLocal) GetVoteFor() ps.ServerAddr {
    return self.getAddr()
}

func (self *MockLocal) GetLeader() ps.ServerAddr {
    return self.getAddr()
}

func (self *MockLocal) ConfigManager() ps.ConfigManager {
    return self.configManager
}

func (self *MockLocal) Log() ps.Log {
    return self.log
}

func (self *MockLocal) SnapshotManager() ps.SnapshotManager {
    return self.snapshotManager
}

func (self *MockLocal) Notifier() *Notifier {
    return self.notifier
}

func TestPeer(_ *testing.T) {
    // TODO add impl
}
