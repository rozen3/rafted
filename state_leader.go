package rafted

import "fmt"
import hsm "github.com/hhkbp2/go-hsm"

type Leader struct {
    hsm.StateHead
}

func NewLeader(super hsm.State) *Leader {
    object := &Leader{hsm.MakeStateHead(super)}
    super.AddChild(object)
    return object
}

func (*Leader) ID() string {
    return LeaderID
}

func (self *Leader) Entry(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Entry")
    return nil
}

func (self *Leader) Exit(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Exit")
    return nil
}

func (self *Leader) Handle(sm hsm.HSM, event hsm.Event) (state hsm.State) {
    fmt.Println(self.ID(), "-> Handle, event=", event)
    switch event.Type() {
    case EventRequestVoteRequest:
        // TODO DEBUG
        fmt.Println("Leader possible step down")
        return nil
    case EventReadRequest:
        fmt.Println("Leader process request")
        return nil
    }
    return self.Super()
}
