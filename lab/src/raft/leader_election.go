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
	fmt.Printf("@@@ rf.me = %d to follower @@@\n", rf.me)
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
}

func (rf *Raft) toLeader() {
	fmt.Printf("@@@ rf.me = %d to leader @@@\n", rf.me)
	rf.state = LEADER
	rf.leader = rf.me
	rf.currentTerm++  // Increment currentTerm

	// clear states
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// send heartbeat
	rf.sendHeartbeat()
}

func (rf *Raft) vote(voteChan chan int, replies []*RequestVoteReply) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

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
			ok := rf.sendRequestVote(i, &args, replies[i])
			if ok {
				fmt.Printf("term = %d, server %d send request vote to %d\n", rf.currentTerm, rf.me, i)
				voteChan <- i
			}
		}
	}
}

func (rf *Raft) countVotes(voteChan chan int, replies []*RequestVoteReply) {
	votes := 1  // vote for self
	for _ = range replies {
		peer := <-voteChan
		reply := replies[peer]
		fmt.Printf("=== peer %d votes to server %d: %t ===\n", peer, rf.me, reply.VoteGranted)

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
			// close(voteChan)
		}
	}
	return
}

func (rf *Raft) runLeaderElection() {
	rf.toCandidate()

	fmt.Printf("!!!!! NEW ELECTION !!!!! me = %d\n", rf.me)
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
	reply := AppendEntriesReply{}
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendEntries(i, &args, &reply)
			if !reply.Success {
				fmt.Printf("### server %d to follower ###\n", rf.me)
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
		// If a follower receives no communication over a period of time called the election timeout,
		// then it assumes there is no viable leader and begins an election to choose a new leader.
		electionTimeout := time.Duration(100 + rf.me * 200 + rand.Intn(200)) * time.Microsecond
		<-time.After(electionTimeout)
		// fmt.Printf("===server %d status %s===\n", rf.me, rf.state)
		if rf.state != LEADER {
			isFirstRound = !isFirstRound
			if isFirstRound {
				rf.votedFor = -1
				continue
			}
			// fmt.Println(rf.me, electionTimeout)
			if time.Now().Sub(rf.lastHeartBeat) >= electionTimeout {
				go rf.runLeaderElection()
				time.Sleep(1 * time.Second)
			}
		} else {
			rf.sendHeartbeat()
		}
	}
}
