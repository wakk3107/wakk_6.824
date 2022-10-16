package raft

func (rf *Raft) doInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.status != leader {
		DPrintf("S%d status change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.frontLog().Index,
		LastIncludedTerm:  rf.frontLog().Term,
	}

	args.Data = rf.persister.ReadSnapshot()
	//copy(args.Data, rf.persister.ReadSnapshot())
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// status changed or outdue data, ignore
	if rf.currentTerm != args.Term || rf.status != leader || reply.Term < rf.currentTerm {
		// overdue, ignore
		DPrintf("S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("S%d S%d term larger(%d > %d)", rf.me, peer, args.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
		return
	}

	rf.nextIndex[peer] = args.LastIncludedIndex + 1

	DPrintf("S%d send snapshot to C%d success!", rf.me, peer)
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d S%d installSnapshot", rf.me, args.LeaderId)
	defer DPrintf("S%d arg: %+v reply: %+v", rf.me, args, reply)

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

	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("S%d args's snapshot too old(%d < %d)", rf.me, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d CondInstallSnapshot(lastIncludedTerm: %d lastIncludedIndex: %d lastApplied: %d commitIndex: %d)", rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied, rf.commitIndex)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("S%d refuse, snapshot too old(%d <= %d)", rf.me, lastIncludedIndex, rf.frontLogIndex())
		return false
	}

	if lastIncludedIndex > rf.lastLogIndex() {
		rf.log = make([]Entry, 1)
	} else {
		// in range, ignore out of range error
		idx, _ := rf.transfer(lastIncludedIndex)
		entries := make([]Entry, 0)
		for i := idx; i < len(rf.log); i++ {
			entries = append(entries, rf.log[i])
		}
		rf.log = entries
	}
	// dummy node
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Cmd = nil

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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d call Snapshot, index: %d", rf.me, index)

	// refuse to install a snapshot
	if rf.frontLogIndex() >= index {
		DPrintf("S%d refuse, have received %d snapshot", rf.me, index)
		return
	}

	idx, err := rf.transfer(index)
	if err < 0 {
		idx = len(rf.log) - 1
	}
	//before := len(rf.log)
	// let last snapshot node as dummy node
	entries := make([]Entry, 0)
	for i := idx; i < len(rf.log); i++ {
		entries = append(entries, rf.log[i])
	}
	rf.log = entries
	//不要这样：rf.log = rf.log[idx:]， 持有引用 会导致被压缩掉的log不会被gc自动释放
	rf.log[0].Cmd = nil // dummy node
	rf.persistSnapshot(snapshot)
	//fmt.Printf("S%d idx: %d log len before: %d after: %d\n", rf.me, idx, before, len(rf.log))
	// DPrintf("S%d call Snapshot success, index: %d {%+v}", rf.me, index, rf.log)
}
