package raft

import (
	"time"
)

func (rf *Raft) toCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++  // Increment currentTerm
	rf.votedFor = rf.me  // Vote for self
}

func (rf *Raft) runLeaderElection() {
	rf.toCandidate()

	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers) - 1)

	// TODO(ling): Implement vote
}

// Kick off leader election periodically by sending out RequestVote RPCs
// when it hasn't heard from another peer for a while.
func (rf *Raft) run() {
	for {
		rf.Lock()
		defer rf.Unlock()

		electionTimeout := 300 + time.Duration(rand.Intn(150)) * time.Microsecond
		clock := time.NewTimer(electionTimeout)

		<-clock.C
		if rf.state != LEADER {
			go rf.runLeaderElection()
		} else {
			// TODO(ling): send heartbeat
		}
	}
}
