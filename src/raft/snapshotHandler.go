package raft

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
