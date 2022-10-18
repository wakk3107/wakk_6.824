# 总体概览
lab2 的内容是要实现一个除了节点变更功能外的 raft 算法，还是比较有趣的。
它被划分成了Lab2A、Lab2B、Lab2C和Lab2D四个子任务：

1. Lab2A：实现 leader election、heartbeat。
2. Lab2B：实现 Log replication。
3. Lab2C：实现 state persistent。
4. Lab2D：实现 SnapShot


有关 go 实现 raft 的种种坑，可以首先参考 6.824 课程对[locking](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt) 和 [structure](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt) 的描述，然后再参考 6.824 TA 的 [guidance](https://thesquareplanet.com/blog/students-guide-to-raft/) 。写之前一定要看看这三篇博客，否则很容易被 bug 包围。

实现Raft的时候基本就盯着Figure2的图片即可：
![Raft_rpc](../../img/Raft_rpc.png)

## 关于 LogIndex
raft 关于一致性最重要的就是 peers 之间各种 Index 的同步。但需要注意的一点是：**logEntry 的 Index 是从 1 开始的**，而不同与log[]的数组下标。在完成 D Snapshot时，需要在数据中截断Index小于SnapShotIndex的Entry，而只保留Index大于的Entry在log[]数组中，因此，我们要正确区分 EntryIndex (包括figure2中提及的所有Index)和 ArrayIndex (用作下表去取log[]中的Entry）。因此关于 Log数组中下标为0的位置，我拿来作为个 dummy node,用于lab4D中的 lastSnapShotIndex 和 lastSnapShotTerm，这样后续的代码会有一定的简便性。

# lab4D
这一部分要求我们为 raft 添加日志压缩功能：在运行一段时间后，raft 的上层 service 可以生成一个 snapshot ，并通知 raft 。在这之后，raft 就可以丢弃形成该 snapshot 的 log entries，起到节约空间的作用。

这里需要仔细阅读论文section 7,理解如何通过SnapShot 来实现log compaction,限制无限制的log增长，减轻peers的存储压力。以及这种方式的优点。

# 关于Snapshot()
`Snapshot(index int, snapshot []byte)` 由状态机调用，传入的index表示 lastIncludeIndex ，snapshot 由状态机生成(应用层)，发送给 Raft 保存，用于发送 Follower 时需要。
```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// refuse to install a snapshot
    // dummy node
    //该快照过时了
	if rf.frontLogIndex() >= index {
		DPrintf("S%d refuse, have received %d snapshot", rf.me, index)
		return
	}

	idx, err := rf.transfer(index)
	if err < 0 {
		idx = len(rf.log) - 1
	}

	// let last snapshot node as dummy node
	rf.log = rf.log[idx:]
	rf.log[0].Cmd = nil // dummy node
    //发送给 Raft 层保存
	rf.persistSnapshot(snapshot)
}
```
# 关于 CondInstallSnapshot()
看过 Lab2D 实验指导的人都会发现，如果 follower 收到了一个 InstallSnapshot RPC ，其处理逻辑是非常扭曲的：

首先，在 InstallSnapshot Handler 中，follower 需要将收到的 snapshot 通过 applyCh 发送给上层 service 。此时 follower 并不会安装这个 snapshot。

在一段时间后，上层 service 会调用 CondInstallSnapshot 函数，询问raft是否应该安装此 snapshot 。若在 follower 执行 InstallSnapshot Handler 到执行 CondInstallSnapshot 的这段时间里，raft 若因为收到 applyentries RPC 导致其 commitID 超过该 snapshot，则应该拒绝安装该快照。

# InstallSnapshot RPC
首先是 doAppendEntries() 应该新增如下逻辑，当 Leader 中应被同步到 Follower 的日志在快照中时，将快照发送给 Follower ,否则就按线性一致性同步 log 。

```go
		if wantSendIndex < rf.frontLogIndex() {
			go rf.doInstallSnapshot(i)
		} else {
			go rf.appendTo(i)
		}
```

`doInstallSnapshot()`和发送日志序列类似

```go
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return ok
	}
	return ok
}
```
`InstallSnapshot()`和`AppendEntries()`类似，`args.LastIncludedIndex <= rf.commitIndex`也是一样的，表示一个旧的快照。

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
	}

	if rf.status != follower {
		rf.TurnTo(follower)
	}

	reply.Term = rf.currentTerm
	rf.resetElectionTime()
    //旧快照
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
    //去达成共识，保障线性一致性且发送给状态机
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
```

注意：快照是状态机中的概念，需要在状态机中加载快照，因此要通过 applyCh 将快照发送给状态机，但是发送后 Raft 并不立即保存快照，而是等待状态机调用 CondInstallSnapshot() ，如果从收到 InstallSnapshot() 后到调用CondInstallSnapshot() 前，没有新的日志提交到状态机，则 Raft 返回 True ，Raft 和状态机保存快照，否则 Raft 返回 False ，两者都不保存快照。（rf.applyCh <- ApplyMsg 是有可能阻塞的）

如何判断 InstallSnapshot() 到 CondInstallSnapshot() 之间没有新的日志提交到状态机呢？这里使用 commitIndex 来判断，当 lastIncludeIndex <= commitIndex 时，说明这期间原本没有的快照部分的日志补全了，虽然 commit 状态并不一定是 apply 状态，但这里以 commit 为准，更安全。


```go
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
    //不能保存该快照，有新日志到达
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.lastLogIndex() {
		rf.log = make([]Entry, 1)
	} else {
		// in range, ignore out of range error
		idx, _ := rf.transfer(lastIncludedIndex)
		rf.log = rf.log[idx:]
	}
	// dummy node
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Cmd = nil
    //保存该快照
	rf.persistSnapshot(snapshot)
	// reset commit
	if lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = lastIncludedIndex
	}
	if lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = lastIncludedIndex
	}

	return true
}

```