package raft

func (rf *Raft) SetHeatBeatClock() {
	rf.heartBeatClock.Reset(rf.clockInterval*time.Millisecond)
}

func (rf *Raft) SendHeartbeat() {
	// send heartbeat to all other followers
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
