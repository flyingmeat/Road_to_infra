package raft

import "time"

func (rf *Raft) SetHeatBeatClock() {
	rf.heartBeatClock.Reset(rf.clockInterval*time.Millisecond)
}

func (rf *Raft) SendHeartbeat() {
	// Send heartbeat to all other followers
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		go func(currentTerm int, server int) {
			args := AppendEntriesArgs{currentTerm, rf.me, []*LogEntry{}, 0, 0, rf.committedIndex}
			reply := AppendEntriesReply{}
			ok := rf.SendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}
			if !reply.Success && reply.Term > currentTerm {
				rf.BackToFollower(reply.Term)
			}
		}(currentTerm, server)
	}
}

func (rf *Raft) LeaderElection() {
	for {
		select {
			case <- rf.killChan:
				return
			case <- rf.heartBeatClock.C:
				rf.SetHeatBeatClock()
				_, isLeader := rf.GetState()
				if !isLeader {
					// if time out start election round
					rf.mu.Lock()
					rf.onEmptyElectionTerm = !rf.onEmptyElectionTerm
					// if candidate and started a vote previous round , close previous vote
					if rf.status == candidate && rf.onEmptyElectionTerm{
						rf.votedFor = -1
						rf.closeVoteChan <- true
					}
					rf.mu.Unlock()

					if rf.onEmptyElectionTerm {
						continue
					}
					// start election
					// TODO(heng): call starting election
				} else {
					// else send heartbeat
					rf.SendHeartbeat()
				}
		}
	}
}
