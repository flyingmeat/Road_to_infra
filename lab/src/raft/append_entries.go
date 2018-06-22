package raft

// AppendEntry logic

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	Entries []*LogEntry
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || !rf.IsLogConsecutive(){
		reply.Success = false
		return
	}

	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.logs))
	}

	if rf.committedIndex > rf.lastApplied {
		// Apply all the un-applied command here
		for i := rf.lastApplied + 1; i <= rf.committedIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.logs[i - 1].Command, rf.logs[i - 1].Index}
		}
		rf.lastApplied = rf.committedIndex
	}

	if len(args.Entries) == 0 {
		// Handle heartbeat
		reply.Success = true
		rf.BackToFollower(args.Term)
	}

	if args.PrevLogIndex <= 0 || rf.logs[args.PrevLogIndex - 1].Term == args.PrevLogTerm {
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			entry := args.Entries[i]
			if entry.Index <= len(rf.logs) {
				if rf.logs[entry.Index - 1].Term != entry.Term {
					rf.logs = rf.logs[:entry.Index - 1]
				} else {
					continue
				}
			}
			rf.logs = append(rf.logs, entry)
		}
	} else {
		reply.Success = false
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) IsLogConsecutive() bool {
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i] == nil {
			return false
		}
	}
	return true
}
