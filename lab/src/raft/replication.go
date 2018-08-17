package raft

import "fmt"

func (rf *Raft) retry(peer int, req *AppendEntriesArgs, res *AppendEntriesReply) {
	ok := false
	for !ok {
		ok = rf.sendAppendEntries(peer, req, res)
	}
}


func (rf *Raft) replicate(replicateChan chan int, replies []*AppendEntriesReply) {
	lastLogIndex := 0
	lastLogTerm := 0
	if lastLog := getLastLog(rf.log); lastLog != nil {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	for i := range rf.peers {
		if (i != rf.me) {
			if rf.state != LEADER {
				return
			}
			
			// TODO: still send, though the majority are committed
			logStartIndex := rf.commitIndex
			if logStartIndex >= len(rf.log) {
				continue
			}
			args := AppendEntriesArgs{
				Term:             rf.currentTerm,
				Leader:           rf.me,
				PrevLogIndex:     lastLogIndex,
				PrevLogTerm:      lastLogTerm,
				Entries:          rf.log[logStartIndex:],
				LeaderCommit:     rf.commitIndex,
			}

			// TODO: try matchIndex ~ nextIndex
			rf.retry(i, &args, replies[i])
			for !replies[i].Success && logStartIndex > 0 {
				// rf.toFollower(replies[i].Term)
				logStartIndex--
				args.Entries = rf.log[logStartIndex:]
				rf.retry(i, &args, replies[i])
			}
			replicateChan <- i
		}
	}
}

func (rf *Raft) countReplicas(replicateChan chan int, replies []*AppendEntriesReply) {
	replicas := 1  // replicate for self
	for _ = range replies {
		fmt.Printf("=== countReplicas: rf.me = %d, rf.state = %s ===\n", rf.me, rf.state)
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
		}

		// If replicas received from majority of servers: apply
		fmt.Printf("=== %d replicas for rf.me = %d ===\n", replicas, rf.me)
		isMajority := replicas >= len(replies) / 2
		if (isMajority && rf.state == LEADER) {
			// TODO: update matchIndex & nextIndex
			rf.commitIndex = len(rf.log)
		}
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
