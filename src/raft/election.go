package raft

// ticker() call doElection(), ticker() hold lock
func (rf *Raft) doElection() {
	votedcount := 1
	entry := rf.lastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: entry.Index,
		LastLogTerm:  entry.Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != args.Term || rf.status != candidate {
				// election timeout, re-election
				// ignore it
				return
			}

			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				DPrintf("S%d S%d term larger(%d > %d)", rf.me, i, args.Term, rf.currentTerm)
				// turn to follower
				rf.currentTerm, rf.votedFor = reply.Term, voted_nil
				rf.persist()
				rf.TurnTo(follower)
				return
			}

			if reply.VoteGranted {
				votedcount++
				// If votes received from majority of servers: become leader
				if votedcount > len(rf.peers)/2 && rf.status == candidate {
					rf.TurnTo(leader)
				}
			}
		}(i)
	}
}

// handler need to require lock
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("vote request: term %d;  %d request to be voted\n", args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d C%d asking vote", rf.me, args.CandidateId)

	defer rf.persist()

	if args.Term < rf.currentTerm { // ignore
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("S%d Term is higher than C%d, refuse it", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		DPrintf("S%d Term is lower than C%d, turn to follower && reset voted_for", rf.me, args.CandidateId)
		rf.TurnTo(follower)
		// can vote now
	}

	if rf.votedFor == voted_nil || rf.votedFor == args.CandidateId { // haven't voted
		// log judge
		if !rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted, reply.Term = false, rf.currentTerm
			DPrintf("S%d C%d not up-to-date, refuse it{arg:%+v, index:%d term:%d}", rf.me, args.CandidateId, args, rf.lastLogIndex(), rf.lastLog().Term)
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//  prevent election timeouts (§5.2)
		DPrintf("S%d Granting Vote to S%d at T%d", rf.me, rf.votedFor, rf.currentTerm)
		rf.resetElectionTime()
		return
	}

	// have voted
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DPrintf("S%d Have voted to S%d at T%d, refuse S%d", rf.me, rf.votedFor, rf.currentTerm, args.CandidateId)
	return
}
