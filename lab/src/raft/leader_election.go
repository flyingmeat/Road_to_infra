package raft

import (
	"time"
)

func (rf *Raft) toCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++  // Increment currentTerm
	rf.votedFor = rf.me  // Vote for self
}

func (rf *Raft) toFollower(currentTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
}

func (rf *Raft) toLeader() {
	rf.state = LEADER
	rf.leader = rf.me

	// clear states
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// send heartbeat
	rf.sendHeartbeat()
}

func (rf *Raft) vote(voteChan chan int, replies []RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := getLastLog(rf.log)
	lastLogIndex, lastLogTerm := -1, -1
	if (lastLog != nil) {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	for i := range rf.peers {
		if (i != rf.me) {
			go rf.sendRequestVote(i, &args, &replies[i])
			voteChan <- i
		}
	}
}

func (rf *Raft) countVotes(voteChan chan int, replies []RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	votes := 1  // vote for self
	for _ = range replies {
		peer := <-voteChan
		reply := replies[peer]

		// If AppendEntries RPC received from new leader: convert to follower
		if (reply.Term > rf.currentTerm) {
			rf.toFollower(reply.Term)
			return
		}
		if (reply.VoteGranted) {
			votes += 1
		}

		// If votes received from majority of servers: become leader
		isMajority := votes > len(replies) / 2
		if (isMajority && rf.state == CANDIDATE) {
			rf.toLeader()
			close(voteChan)
		}

		// If election timeout elapses: start new election?
	}
	return
}

func (rf *Raft) runLeaderElection() {
	rf.toCandidate()

	voteChan := make(chan int, len(rf.peers) - 1)
	replies := make([]RequestVoteReply, len(rf.peers))
	for i, _ := range replies {
		replies[i] = &RequestVoteReply{}
	}

	rf.vote(voteChan, replies)
	rf.countVotes(voteChan, replies)
}

func (rf *Raft) sendHeartbeat() {
	lastLog := getLastLog(rf.log)
	args := AppendEntriesArgs{
		Term:             rf.currentTerm,
		Leader:           rf.me,
		PrevLogIndex:     lastLog.Index,
		PrevLogTerm:      lastLog.Term,
		Entries:          []LogEntry{},
		LeaderCommit:     rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendEntries(i, &args, &reply)
			if !reply.Success {
				rf.toFollower(reply.Term)
			}
		}
	}
}

// Kick off leader election periodically by sending out RequestVote RPCs
// when it hasn't heard from another peer for a while.
func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// If a follower receives no communication over a period of time called the election timeout,
		// then it assumes there is no viable leader and begins an election to choose a new leader.
		electionTimeout := 300 + time.Duration(rand.Intn(150)) * time.Microsecond
		<-time.After(electionTimeout)
		if rf.state != LEADER {
			if time.Now().Sub(rf.lastHeartBeat) >= electionTimeout {
				go rf.runLeaderElection()
			}
		} else {
			rf.sendHeartbeat()
		}
	}
}
