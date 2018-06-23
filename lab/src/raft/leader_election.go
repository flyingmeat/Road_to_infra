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

func (rf *Raft) runLeaderElection() {
	rf.toCandidate()

	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers) - 1)

	// TODO(ling): Implement vote
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
