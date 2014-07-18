package rafted

type StateMachine interface {
    Apply([]byte) error
}

type RaftFactory interface {
    New(sm StateMachine) (Raft, error)
}

type Raft interface {
    Append([]byte) error
}

type DummyRaftFactory struct {
}

func NewDummyRaftFactory() *DummyRaftFactory {
    return &DummyRaftFactory{}
}

func (self *DummyRaftFactory) New(sm StateMachine) (Raft, error) {
    return &DummyRaft{sm}, nil
}

type DummyRaft struct {
    sm StateMachine
}

func (self *DummyRaft) Append(b []byte) error {
    return self.sm.Apply(b)
}

type DummyStateMachine struct {
}

func NewDummyStateMachine() *DummyStateMachine {
    return &DummyStateMachine{}
}

func (self *DummyStateMachine) Apply(_ []byte) error {
    return nil
}

// func usage() {
//  dummy := NewDummyStateMachine()
//  raftFactory := NewDummyRaftFactory()
//  raft := raftFactory.New(dummy)
//  raft.Append(...)
// }
