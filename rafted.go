package rafted

func CreateRaftHSM(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    localAddr net.Addr) *RaftHSM {

    top := hsm.NewTop()
    initial := hsm.NewInitial(top, FollowerID)
    NewFollower(top, heartbeatTimeout)
    NewCandidate(top, electionTimeout)
    NewLeader(top)
    raftHSM := NewRaftHSM(top, initial, localAddr)
    raftHSM.Init()
    return raftHSM
}

type RaftNode struct {
    raftHSM     *RaftHSM
    peerManager *PeerManager
    client      *network.Client
    server      *network.Server
}

func NewRaftNode(
    heartbeatTimeout time.Duration,
    electionTimeout time.Duration,
    poolSize uint32,
    localAddr net.Addr,
    bindAddr net.Addr,
    peerAddrs []net.Addr) *RaftNode {

    raftHSM := NewRaftHSM(heartbeatTimeout, electionTimeout, localAddr)
    client := network.NewSocketClient(poolSize)
    eventHandler1 := func(event RaftEvent) {
        raftHSM.Dispatch(event)
    }
    eventHandler2 := func(event RequestEvent) {
        raftHSM.Dispatch(event)
    }
    peerManager := NewPeerManager(peerAddrs, client, eventHandler1)
    raftHSM.SetPeerManager(peerManager)
    server := network.NewSocketServer(bindAddr, eventHandler2)
    return &RaftNode{
        raftHSM:     raftHSM,
        peerManager: peerManager,
        client:      client,
        server:      server,
    }
}

func (self *RaftNode) Run() {
    self.server.Serve()
}
