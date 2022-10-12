package raft

// ticker() call doAppendEntries(), ticker() hold lock
// if a node turn to leader, leader will call doAppendEntries() to send a heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendTo(i)

	}
}

func (rf *Raft) appendTo(peer int) {
	rf.mu.Lock()
	if rf.status != leader {
		DPrintf("S%d status change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: magic_index,
		PrevLogTerm:  magic_term,
		LeaderCommit: rf.commitIndex,
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	idx, err := rf.transfer(prevLogIndex)
	if err < 0 {
		rf.mu.Unlock()
		return
	}

	args.PrevLogIndex = rf.log[idx].Index
	args.PrevLogTerm = rf.log[idx].Term

	// must copy in here
	entries := rf.log[idx+1:]
	args.Entries = make([]Entry, len(entries))
	copy(args.Entries, entries)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, &args, &reply)
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

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		DPrintf("S%d S%d term larger(%d > %d)", rf.me, peer, reply.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
		return
	}

	if reply.Success {
		// utils.Debug(utils.DTrace, "S%d before nextIndex:{%+v} ", rf.me, rf.nextIndex)
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		// utils.Debug(utils.DTrace, "S%d after nextIndex:{%+v}", rf.me, rf.nextIndex)
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.toCommit()
		return
	}

	if reply.XTerm == -1 { // null slot
		rf.nextIndex[peer] -= reply.XLen
	} else if reply.XTerm >= 0 {
		termNotExit := true
		for index := rf.nextIndex[peer] - 1; index >= 1; index-- {
			entry, err := rf.getEntry(index)
			if err < 0 {
				continue
			}

			if entry.Term > reply.XTerm {
				continue
			}

			if entry.Term == reply.XTerm {
				rf.nextIndex[peer] = index + 1
				termNotExit = false
				break
			}
			if entry.Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[peer] = reply.XIndex
		}
	} else {
		rf.nextIndex[peer] = reply.XIndex
	}

	// utils.Debug(utils.DTrace, "S%d nextIndex:{%+v}", rf.me, rf.nextIndex)
	// the smallest nextIndex is 1
	// otherwise, it will cause out of range error
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// handler need to require lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d S%d appendEntries", rf.me, args.LeaderId)
	defer DPrintf("S%d arg: %+v reply: %+v", rf.me, args, reply)

	defer rf.persist()

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		DPrintf("S%d S%d term larger(%d > %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.TurnTo(follower)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	//  prevent election timeouts (§5.2)
	rf.resetElectionTime()

	//日志断层
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex()
		return
	}

	idx, err := rf.transfer(args.PrevLogIndex)
	if err < 0 {
		return
	}

	if rf.log[idx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[idx].Term
		reply.XIndex = args.PrevLogIndex
		// 0 is a dummy entry => quit in index is 1
		// binary search is better than this way
		for index := idx; index >= 1; index-- {
			if rf.log[index-1].Term != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		if rf.isConflict(args) {
			rf.log = rf.log[:idx+1]
			entries := args.Entries
			rf.log = append(rf.log, entries...)
			// DPrintf("S%d conflict, truncate log: %+v", rf.me, rf.log)
		} else {
			// DPrintf("S%d no conflict, log: %+v", rf.me, rf.log)
		}
	} else {
		DPrintf("S%d args entries nil or length is 0: %v", rf.me, args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		DPrintf("S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
	// DPrintf("S%d log: %+v", rf.me, rf.log)
}

func (rf *Raft) isConflict(args *AppendEntriesArgs) bool {
	base_index := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		entry_rf, err := rf.getEntry(i + base_index)
		if err < 0 || entry_rf.Term != entry.Term {
			return true
		}
	}
	return false
}
