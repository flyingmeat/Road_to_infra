package raft

func (rf *Raft) SetHeatBeatClock() {
	rf.heartBeatClock.Reset(rf.clockInterval*time.Millisecond)
}

func (rf *Raft) SendHeartbeat() {
	// send heartbeat to all other followers
}

func (rf *Raft) BackToFollower(newTerm int) {
	rf.mu.Lock()
	rf.currentTerm = newTerm
	rf.votedFor = -1
	if rf.status == candidate {
		rf.closeVoteChan <- true
	} else if rf.status == leader {
		rf.clockInterval = time.Duration(rand.Intn(150) + 200 + (rf.me*100)%150)
	}
	rf.status = follower
	rf.onEmptyElectionTerm = false
	rf.SetHeatBeatClock()
	rf.mu.Unlock()
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
					rf.voteChan = make(chan int)
					rf.StartElection()
				} else {
					// else send heartbeat
					rf.SendHeartbeat()
				}
		}
	}
}

func (rf *Raft) StartElection() {
	// There are 3 situations that starte election should be terminated:
	// 1. Vote count does not reach the half of total number of
	// 	  servers, and timeout
	// 2. Receive heartbeat from higher term leader
	// 3. local next term is smaller than other server, known by requestVoteReply

	rf.mu.Lock()
	newTerm := rf.currentTerm + 1
	rf.status = candidate
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// channel used to close the send vote request proccesses
	closeSendVoteRequestChan := make(chan bool, 1)

	// Start send vote request and counting vote
	go rf.StartSendVoteRequest(newTerm, closeSendVoteRequestChan)

	go rf.CountVote(newTerm, closeSendVoteRequestChan)

}

func (rf *Raft) StartSendVoteRequest(newTerm int, closeSendVoteRequestChan chan bool) {
	// if the send vote request proccess is closed, this will tell
	// all the remain send vote processes to stop and not send vote to count vote channel anymore
	isSendVoteClose := false

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int, closeSendVoteRequestChan chan bool, isSendVoteClose *bool) {
			rf.mu.Lock()
			localLastLogIndex := len(rf.log)
			localLastLogTerm := 0
			if localLastLogIndex > 0 {
				localLastLogTerm = rf.log[localLastLogIndex - 1].Term
			}
			rf.mu.Unlock()

			requestVoteArgs := RequestVoteArgs{newTerm, rf.me, lastLogIndex, lastLogTerm}
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if ok {
				if requestVoteReply.VoteGranted && !*isSendVoteClose {
					select {
						case <- closeSendVoteRequestChan:
							*isSendVoteClose = true
							close(rf.voteChan)
						case rf.voteChan <- 1:
							return
					}
				} else if requestVoteReply.Term > rf.currentTerm {
					// Back to follower
					rf.BackToFollower(requestVoteReply.Term)
				}
			}
		} (server, closeSendVoteRequestChan, &isSendVoteClose)
	}
}

func (rf *Raft) CountVote(newTerm int, closeSendVoteRequestChan chan bool) {
	voteCount := 1
	for {
		select {
			case _, ok := <- rf.voteChan:
				if !ok {
					break
				}
				voteCount++
				if voteCount > len(rf.log) / 2 {
					// Change to leader
					rf.mu.Lock()
					rf.status = leader
					rf.currentTerm = newTerm
					rf.clockInterval = time.Duration(rand.Intn(50) + 100)
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex = append(rf.nextIndex, len(rf.logs) + 1)
						rf.matchIndex = append(rf.matchIndex, 0)
					}
					rf.SetHeatBeatClock()
					rf.mu.Unlock()
					closeSendVoteRequestChan <- true
					rf.SendHeartbeat()
					return
				}
			case <- rf.closeVoteChan:
				closeSendVoteRequestChan <- true
				return
			default:
				if rf.status != candidate {
					break
				}
				continue
		}
	}
}

