package raft

// ticker() call doAppendEntries(), ticker() hold lock
// if a node turn to leader, leader will call doAppendEntries() to send a heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wantSendIndex := rf.nextIndex[i] - 1
		// 若需要的 log 索引已经被快照存储了，这时直接发送快照
		if wantSendIndex < rf.frontLogIndex() {
			go rf.doInstallSnapshot(i)
		} else {
			go rf.appendTo(i)
		}
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
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.toCommit()
		return
	}
	// https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-07-raft2/7.3-hui-fu-jia-su-backup-acceleration

	// case 3 :没发生冲突，但是发少了
	if reply.XTerm == -1 { // null slot
		rf.nextIndex[peer] -= reply.XLen
	} else if reply.XTerm >= 0 { // 发生了冲突，要 XTerm 所对应的 log 及之后的

		termNotExit := true
		// 开始递减 nextIndex
		for index := rf.nextIndex[peer] - 1; index >= 1; index-- {
			entry, err := rf.getEntry(index)
			if err < 0 {
				continue
			}
			// 若比 XTerm 大，则直接遍历下一个
			if entry.Term > reply.XTerm {
				continue
			}
			// case 2: 找到 XTerm 对应的 log 了，取下一个，即第一个大于 XTerm的 log，这时如果再发起 append 请求，prelogindex就会相等了
			if entry.Term == reply.XTerm {
				rf.nextIndex[peer] = index + 1
				termNotExit = false
				break
			}
			// case 1:若都遍历到比 XTerm 小的日志仍然没有发现 XTerm 对应的日志，则尝试去覆盖 XTerm 开始的索引
			if entry.Term < reply.XTerm {
				break
			}
		}
		// 没有发现 XTerm 对应的日志，则尝试去覆盖 XTerm 开始的索引
		if termNotExit {
			rf.nextIndex[peer] = reply.XIndex
		}
	} else {
		rf.nextIndex[peer] = reply.XIndex
	}
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
	// 发过来的日志在快照里
	if args.PrevLogIndex < rf.frontLogIndex() {
		reply.XTerm, reply.XIndex, reply.Success = -2, rf.frontLogIndex()+1, false
		DPrintf("S%d args's prevLogIndex too smaller(%v < %v)", rf.me, args.PrevLogIndex, rf.frontLogIndex())
		return
	}
	// 日志断层
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
	// 如果发过来的日志，Term 不匹配，请求覆盖
	if rf.log[idx].Term != args.PrevLogTerm {
		reply.Success = false
		// 设置 XTerm 为 PrevLogIndex 对应的 term
		reply.XTerm = rf.log[idx].Term
		reply.XIndex = args.PrevLogIndex
		// 0 is a dummy entry => quit in index is 1
		// binary search is better than this way
		// 找 XTerm 对应的第一个日志的索引
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
