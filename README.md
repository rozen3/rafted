rafted
======


```rafted``` is a Golang Library that implements the Raft distributed consensus protocol.

```Raft``` is an understandable consensus protocol which reaches distributed consistency by using replicated logs and state machines among a cluster of nodes.

For more details on ```Raft```, please refer to the paper [In Search of an Understandable Consensus Algorithm][raft-paper] and the [Raft Consensus homepage][raft-homepage].

## Another Raft Implementation

There is a list of Raft implemetations on the [Raft website][raft-website], where you could find dozens of Raft implementations written in various programming languages. 

At the moment this project is created, there is at least 5 implementations written in Golang. Among them these two projects are much better written and maintained:

1. [```go-raft```][go-raft-github]
2. [```hashicorp-raft```][hashicorp-raft-github]

After taking a close look into the existing raft implementation projects, we find some issues as the following. It is not easy to change these existing projects to meet our needs. And then we decides to give a shot for a new implementation.

#### 1. Not Full Feature

The existing implemenations don't provide all features described in Raft paper. These features include

1. Leader Election, Log Replication
2. Membership Change
3. Log Compaction

Most of them implement 1 and 3, and partial 2 or without 2. ```hashicorp-raft``` support add/remove only single node in cluster. However we need the them fully functional dure to our usage occasions. Membership Change support on adding/removing multiple nodes is useful to simply the procedure of tablet group changing on our storage service.

#### 2. No Pluggable Network Layer

[```etcd```][etcd-github] is an distributed key value store to maintain cluster configuratin, which is powered by [CoreOS][coreos-mainpage]. It use the ```go-raft``` library as backend to ensure the distributed consistency.

After running some test with ```etcd``` on a three node cluster in our typical production network enviroment, we find the system throughput are at the level of 1000s, which are consitent with the ```etcd``` official benchmark[\[1\]](#1). We feel that throughput is not good enough on our occasions. ```go-raft``` uses HTTP as network protocol, which are concerned when thinking of tuning the performance. 

```hashicorp-raft``` has its network layer implemented in TCP socket, while it doesn't have any similar existing usage project so we don't run the test.

Raft a distributed consensus algorithm, which involves a lot of network interaction. Change in network layer could affect its throughput and timeliness of response. To achieve high performance and short-time responsiveness we need a pluggable network layer, to fine-tune the system by using different network protocol, message serialization and message sending policy.

#### 3. Lack of State Machine

The core of Raft is a cluster of nodes parade in the a time-to-time synchronized pace. During this process, eacho node goes through various states, e.g. Follower, Candidate, Leader, Snapshotting, log-compating, recovering from remote snapshot and so on. And various actions should be taken on the transfer of states. A good pattern to express state and state transfer in software is State Machine.

Both ```go-raft``` and ```hashicorp-raft``` use embedded if-else/switch to implement the states and the actions around states, which is not the best method. In some degree, embedded if-else/switch weakens the code on simplicity and understandability. It's not easy to modify or extand states or actions, along with an intuitional concept of its correctness.

Here I would like to introduce a method called Hierarchical State Machine(HSM) to describe states and their transfering actions for Raft nodes. HSM is a good pattern to express State Machine. It has three main advantages over the traditional methods(such as embedded if-else/switch, jump table, state pattern in OOP):

1. support embedded states and behavior inheritance
2. provide entry and exit actions for state
3. use class hierarchy to express state hierarchy, easy to write and understand

I port HSM into Golang and make it a separate project for code reusage. Please refer to [```go-hsm```][go-hsm-github] for more infos.

## Project Status

This library is under development and far from complete. It would be broken sometimes during the edit-break-fix circle. I would like to stablize the main framework as soon as possible. Any advice or suggestion would be appreciated.

[raft-paper]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

[raft-homepage]: http://raftconsensus.github.io/

[go-raft-github]: https://github.com/goraft/raft

[hashicorp-raft-github]: https://github.com/hashicorp/raft

[etcd-github]: https://github.com/coreos/etcd

[etcd-overview]: https://coreos.com/using-coreos/etcd/

[coreos-mainpage]: https://coreos.com/

[go-hsm-github]: https://github.com/hhkbp2/go-hsm



<a id="1">[1]</a> [etcd Overview][etcd-overview] says: "Benchmarked 1000s of writes/s per instance"


