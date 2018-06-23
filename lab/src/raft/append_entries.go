package raft

import (
	"math"
)

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm

	//  Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.leader == args.Leader
	rf.lastHeartBeat = time.Now()
	if rf.state == CANDIDATE {
		rt.toFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	hasPrevLog := len(rf.log) >= args.PrevLogIndex + 1
	if !hasPrevLog || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	var lastSameIndex int
	for i, leaderEntry := range args.Entries {
		if leaderEntry.Index <= len(rf.log) {
			if leaderEntry.Term != rf.log[leaderEntry.Index - 1].Term {
				rf.log = rf.log[:leaderEntry.Index - 1]
				lastSameIndex = leaderEntry.Index
				break
			}
		}
	}
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[lastSameIndex - 1:]...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if (args.LeaderCommit > rf.commitIndex) {
		lastNewEntryIndex := -1
		lastNewEntry := getLastLog(args.Entries)
		if lastNewEntry != nil {
			lastNewEntryIndex = lastNewEntry.Index
		}
		rf.commitIndex = math.Min(args.LeaderCommit, lastNewEntryIndex)
		// TODO(ling): apply the messages between lastApplied and commitIndex
	}

	reply.Success = true
}
