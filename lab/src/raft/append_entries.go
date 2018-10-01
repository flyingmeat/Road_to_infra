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
	rf.RequestLock([]string{"currentTerm", "committedIndex", "lastApplied"})
	reply.Term = rf.currentTerm


	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.ReleaseLock([]string{"currentTerm", "committedIndex", "lastApplied"})
		return
	}

	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.log))
	}

	if rf.committedIndex > rf.lastApplied {
		// Apply all the un-applied command here
		for i := rf.lastApplied + 1; i <= rf.committedIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.log[i - 1].Command, rf.log[i - 1].Index}
		}
		rf.lastApplied = rf.committedIndex
	}
	rf.ReleaseLock([]string{"currentTerm", "committedIndex", "lastApplied"})
	rf.BackToFollower(args.Term)
	if len(args.Entries) == 0 {
		// Handle heartbeat
		reply.Success = true
		return
	}

	rf.RequestLock([]string{"log"})
	defer rf.ReleaseLock([]string{"log"})
	if rf.IsLogConsecutive() && (args.PrevLogIndex <= 0 || rf.log[args.PrevLogIndex - 1].Term == args.PrevLogTerm) {
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			entry := args.Entries[i]
			if entry.Index <= len(rf.log) {
				if rf.log[entry.Index - 1].Term != entry.Term {
					rf.log = rf.log[:entry.Index - 1]
				} else {
					continue
				}
			}
			rf.log = append(rf.log, entry)
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
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i] == nil {
			return false
		}
	}
	return true
}
