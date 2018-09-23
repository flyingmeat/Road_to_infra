package raft

import "fmt"

func (rf *Raft) retry(peer int, req *AppendEntriesArgs, res *AppendEntriesReply) {
	ok := false
	for !ok {
		ok = rf.sendAppendEntries(peer, req, res)
	}
}


func (rf *Raft) replicate(replicateChan chan int, replies []*AppendEntriesReply) {
	if len(rf.log) == 0 {
		return
	}
	
	lastLogIndex := 0
	lastLogTerm := 0
	if lastLog := getLastLog(rf.log); lastLog != nil {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	fmt.Println("rf.log =", rf.log, "| rf.matchIndex =", rf.matchIndex, "| rf.nextIndex =", rf.nextIndex)
	for i := range rf.peers {
		if (i != rf.me) {
			if rf.state != LEADER {
				return
			}

			args := AppendEntriesArgs{
				Term:             rf.currentTerm,
				Leader:           rf.me,
				PrevLogIndex:     lastLogIndex,
				PrevLogTerm:      lastLogTerm,
				Entries:          rf.log[rf.matchIndex[i]:rf.nextIndex[i]],
				LeaderCommit:     rf.commitIndex,
			}
			fmt.Println("=== to replicate, leader =", rf.me, "| peer =", i, "| args =", args)

			rf.retry(i, &args, replies[i])
			replicateChan <- i
		}
	}
}

func (rf *Raft) countReplicas(replicateChan chan int, replies []*AppendEntriesReply) {
	replicas := 1  // replicate for self
	replicatedPeers := []int{rf.me}

	for i := 1; i < len(replies); i++ {
		if rf.state != LEADER {
			return
		}

		peer := <-replicateChan
		reply := replies[peer]

		// If AppendEntries RPC received from new leader: convert to follower
		if (reply.Term > rf.currentTerm) {
			rf.toFollower(reply.Term)
			return
		}
		if reply.Success {
			replicas += 1
			replicatedPeers = append(replicatedPeers, peer)
		}
	}

	// If replicas received from majority of servers: apply
	isMajority := replicas >= len(replies) / 2
	if (isMajority && rf.state == LEADER) {
		lastLog := getLastLog(rf.log)
		for peer := range replicatedPeers {
			rf.matchIndex[peer] = lastLog.Index
			rf.nextIndex[peer] = lastLog.Index + 1
		}
		for i := rf.commitIndex + 1; i <= lastLog.Index; i++ {
			rf.applyChan <- ApplyMsg{true, lastLog.Command, i}
		}
		rf.commitIndex = lastLog.Index
		fmt.Println("*** replicate for rf.me =", rf.me, "is done:", "rf.matchIndex =", rf.matchIndex, "| rf.nextIndex =", rf.nextIndex, "| rf.commitIndex =", rf.commitIndex)
	}

	return
}


func (rf *Raft) runRaplication() {
	replicateChan := make(chan int, len(rf.peers) - 1)
	replies := make([]*AppendEntriesReply, len(rf.peers))
	for i, _ := range replies {
		replies[i] = &AppendEntriesReply{}
	}

	rf.replicate(replicateChan, replies)
	rf.countReplicas(replicateChan, replies)
}
