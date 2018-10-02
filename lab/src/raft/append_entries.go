package raft

import (
	"fmt"
	"math"
	"time"
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
	fmt.Println("received heartbeat from", args.Leader, "to", rf.me)
	reply.Term = rf.currentTerm

	//  Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.acquireLocks("leader")
	rf.leader = args.Leader
	rf.releaseLocks("leader")
	
	fmt.Println("=== before toFollower from", args.Leader, "to", rf.me)
	rf.toFollower(args.Term)
	fmt.Println("=== after toFollower from", args.Leader, "to", rf.me)

	rf.acquireLocks("lastHeartBeat")
	rf.lastHeartBeat = time.Now()
	rf.releaseLocks("lastHeartBeat")
	
	fmt.Println("finished heartbeat from", args.Leader, "to", rf.me)

	if len(args.Entries) == 0 {
		reply.Success = true
		return
	} 


	rf.acquireLocks("log")
	//fmt.Printf("$$$ AppendEntries from %d to %d: log = %v $$$\n", args.Leader, rf.me, args.Entries)
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	hasPrevLog := len(rf.log) >= args.PrevLogIndex + 1
	if hasPrevLog && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.releaseLocks("log")
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	var lastSameIndex int
	for _, leaderEntry := range args.Entries {
		//fmt.Println("leaderEntry.Index =", leaderEntry.Index, "len(rf.log =", len(rf.log))
		if leaderEntry.Index <= len(rf.log) {
			if leaderEntry.Term != rf.log[leaderEntry.Index - 1].Term {
				rf.log = rf.log[:leaderEntry.Index - 1]
				lastSameIndex = leaderEntry.Index
				break
			}
		}
	}
	// Append any new entries not already in the log
	//fmt.Printf("lastSameIndex = %d, args.Entries = %v\n", lastSameIndex, args.Entries)
	rf.log = append(rf.log, args.Entries[lastSameIndex:]...)
	lastNewEntry := getLastLog(args.Entries)
	//fmt.Printf("rf.me = %d, rf.log = %v\n", rf.me, rf.log)
	rf.releaseLocks("log")

	rf.acquireLocks("commitIndex", "lastApplied")
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if (args.LeaderCommit > rf.commitIndex) {
		lastNewEntryIndex := -1
		if lastNewEntry != nil {
			lastNewEntryIndex = lastNewEntry.Index
		}
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntryIndex)))
	}

	// apply the messages between lastApplied and commitIndex
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyChan <- ApplyMsg{true, lastNewEntry.Command, lastNewEntry.Index}
		}
		rf.lastApplied = rf.commitIndex
	}

	reply.Success = true
	rf.releaseLocks("commitIndex", "lastApplied")
}
