package raft

import "sync"
import "fmt"

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	index = len(rf.logs) + 1
	newEntry := LogEntry{rf.currentTerm, index, command}
	rf.logs = append(rf.logs, &newEntry)
	rf.mu.Unlock()

	fmt.Printf("Start replicate leader %d \n", rf.me)
	rf.Replicate(index, newEntry)

	return index, term, isLeader
}

func (rf *Raft) Replicate(index int, newEntry LogEntry) {

	var locker sync.Mutex
	replicateCount := 1
	cond := sync.NewCond(&locker)

	for server := 0; server < len(rf.peers); server++ {
		if index < rf.nextIndex[server] || server == rf.me{
			continue
		}
		go func(server int) {
			entries := []*LogEntry{&newEntry}
			for {
				if _, isLeader := rf.GetState(); !isLeader {
					return
				}
				if rf.nextIndex[server] != entries[0].Index {
					rf.mu.Lock()
					for i := entries[0].Index - 1; i >= rf.nextIndex[server]; i-- {
						entries = append([]*LogEntry{rf.logs[i - 1]}, entries...)
					}
					rf.mu.Unlock()
				}
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = rf.logs[rf.nextIndex[server] - 1 - 1].Term
				}
				appendEntriesArgs := AppendEntriesArgs{
					rf.currentTerm,
					rf.me, 
					entries,
					prevLogIndex,
					prevLogTerm,
					rf.committedIndex, 
				}
				fmt.Printf("to server %d %+v index %d\n", server, appendEntriesArgs, index)

				appendEntriesReply := AppendEntriesReply{}
				ok := rf.SendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)

				if !ok {
					fmt.Printf("!!!!!%dto server %d failed!!!!!\n", rf.me, server)					
					continue
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if appendEntriesReply.Success {
					locker.Lock()
					replicateCount++
					cond.Signal()
					locker.Unlock()
					rf.nextIndex[server] = newEntry.Index
					if newEntry.Index > rf.matchIndex[server] {
						rf.matchIndex[server] = newEntry.Index
					}
					fmt.Printf("server %d append %d success reply, replicate count %d \n", server, index, replicateCount)
					return
				}

				rf.nextIndex[server]--

			}
		}(server)
	}

	go func(cond *sync.Cond) {
		locker.Lock()
		for !(replicateCount > len(rf.peers) / 2) {
			cond.Wait()
		}
		rf.mu.Lock()
		if rf.currentTerm != rf.logs[index - 1].Term {
			return
		}
		if index > rf.committedIndex {
			for i := rf.committedIndex + 1; i <= index; i++ {
				rf.applyCh <- ApplyMsg{true, rf.logs[i - 1].Command, i}
			}
			rf.committedIndex = index
			fmt.Printf("++++Commit index %d++++\n", index)
		}
		rf.mu.Unlock()
		locker.Unlock()
	}(cond)
}
