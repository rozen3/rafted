*1. redirectable client
2. simple function test
3. log compaction/snapshot compaction routine
4. full feature test
*5. add id for network message  --> rpc does that
6. non-vote replication node for data backup and fast new node bring-up, which includes:
    * read-only replication RPC protocol
    * the jugement of whether remote log has caught up, when to start member change
7. pipeline mode of peer's log replication
8. implementation of LogBarrier entry type
9. read-only implementation
10. read configuration implementation
11. multiple end point network address support
*12. rpc network client/server

