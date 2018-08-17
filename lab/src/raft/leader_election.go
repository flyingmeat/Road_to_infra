package raft

import (
	"math/rand"
	"time"
	"fmt"
)

func (rf *Raft) toCandidate() {
	fmt.Printf("@@@ rf.me = %d to candicate @@@\n", rf.me)
	rf.state = CANDIDATE
	rf.votedFor = rf.me  // Vote for self
}

func (rf *Raft) toFollower(currentTerm int) {
	//fmt.Printf("@@@ rf.me = %d to follower, term = %d @@@\n", rf.me, currentTerm)
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
}

func (rf *Raft) toLeader() {
	rf.state = LEADER
	rf.leader = rf.me
	rf.currentTerm++  // Increment currentTerm
	fmt.Printf("@@@ rf.me = %d to leader, new term = %d @@@\n", rf.me, rf.currentTerm)

	// clear states
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// send heartbeat
	rf.sendHeartbeat()
}

func (rf *Raft) vote(voteChan chan int, replies []*RequestVoteReply) {
	lastLog := getLastLog(rf.log)
	lastLogIndex, lastLogTerm := -1, -1
	if (lastLog != nil) {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}
	args := RequestVoteArgs{
		Term: rf.currentTerm + 1,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	for i := range rf.peers {
		if (i != rf.me) {
			if rf.state != CANDIDATE {
				return
			}

			ok := rf.sendRequestVote(i, &args, replies[i])
			if ok {
				voteChan <- i
			}
		}
	}
}

func (rf *Raft) countVotes(voteChan chan int, replies []*RequestVoteReply) {
	votes := 1  // vote for self
	for _ = range replies {
		fmt.Printf("=== countVotes: rf.me = %d, rf.state = %s ===\n", rf.me, rf.state)
		if rf.state != CANDIDATE {
			return
		}

		peer := <-voteChan
		reply := replies[peer]
		//fmt.Printf("=== peer %d votes to server %d: %t ===\n", peer, rf.me, reply.VoteGranted)

		// If AppendEntries RPC received from new leader: convert to follower
		if (reply.Term > rf.currentTerm) {
			rf.toFollower(reply.Term)
			return
		}
		if (reply.VoteGranted) {
			votes += 1
		}

		// If votes received from majority of servers: become leader
		fmt.Printf("=== %d votes for rf.me = %d ===\n", votes, rf.me)
		isMajority := votes >= len(replies) / 2
		if (isMajority && rf.state == CANDIDATE) {
			rf.toLeader()
			rf.runRaplication()
		}
	}
	return
}

func (rf *Raft) runLeaderElection() {
	rf.toCandidate()

	//fmt.Printf("!!!!! NEW ELECTION !!!!! me = %d, current term = %d, last leader = %d \n", rf.me, rf.currentTerm, rf.leader)
	voteChan := make(chan int, len(rf.peers) - 1)
	replies := make([]*RequestVoteReply, len(rf.peers))
	for i, _ := range replies {
		replies[i] = &RequestVoteReply{}
	}

	rf.vote(voteChan, replies)
	rf.countVotes(voteChan, replies)
}

func (rf *Raft) sendHeartbeat() {
	lastLogIndex := 0
	lastLogTerm := 0
	if lastLog := getLastLog(rf.log); lastLog != nil {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	args := AppendEntriesArgs{
		Term:             rf.currentTerm,
		Leader:           rf.me,
		PrevLogIndex:     lastLogIndex,
		PrevLogTerm:      lastLogTerm,
		Entries:          []LogEntry{},
		LeaderCommit:     rf.commitIndex,
	}
	for i := range rf.peers {
		if i != rf.me {
			reply := AppendEntriesReply{}
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
	isFirstRound := true
	for {
		if rf.state != LEADER {
			// If a follower receives no communication over a period of time called the election timeout,
			// then it assumes there is no viable leader and begins an election to choose a new leader.
			electionTimeout := time.Duration(100 + rf.me * 500 + rand.Intn(200)) * time.Microsecond
			<-time.After(electionTimeout)

			isFirstRound = !isFirstRound
			if isFirstRound {
				rf.votedFor = -1
				continue
			}

			if rf.state == CANDIDATE {
				continue
			}

			gap := time.Now().Sub(rf.lastHeartBeat)
			fmt.Printf("=== me = %d, diff = %v, timeout = %v, isTimeOut = %t, term = %d ===\n", rf.me, gap, electionTimeout, gap >= electionTimeout, rf.currentTerm)
			if gap >= electionTimeout {
				go rf.runLeaderElection()
				time.Sleep(1 * time.Second)
			}
		} else {
			rf.sendHeartbeat()
		}
	}
}
