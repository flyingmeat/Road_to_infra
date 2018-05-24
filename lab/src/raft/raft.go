package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs []*LogEntry
	committedIndex int
	lastApplied int
	applyCh chan ApplyMsg

	nextIndex []int
	matchIndex []int

	heartBeatClock *time.Timer
	clockInterval time.Duration
	voteChan chan int  			   // channel used for voting
	closeVoteChan chan bool
	onEmptyElectionTerm bool       // we are waiting for an empty round for election in order to resolve split vote

	status Status

	currentTerm int
	votedFor int

	killChan chan bool			
}

type Status string

const (
	follower Status = "follower"
	candidate Status = "candidate"
	leader Status = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.status == leader)
	rf.mu.Unlock()
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex - 1].Term
	}

	isTermValid := args.Term >= rf.currentTerm
	isVotedBefore := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	isLogUpdateDate := 
		args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if isTermValid && isVotedBefore && isLogUpdateDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	} 
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	index = len(rf.logs) + 1
	newEntry := LogEntry{rf.currentTerm, index, command}
	rf.logs = append(rf.logs, &newEntry)
	rf.mu.Unlock()

	fmt.Printf("Start replicate leader %d \n", rf.me)
	rf.Replicate(command, index, newEntry)

	return index, term, isLeader
}

func (rf *Raft) Replicate(command interface{}, index int, newEntry LogEntry) {

	var locker sync.Mutex
	replicateCount := 1
	cond := sync.NewCond(&locker)

	for server := 0; server < len(rf.peers); server++ {
		if index < rf.nextIndex[server] || server == rf.me{
			continue
		}
		go func(server int) {
			entries := []*LogEntry{&newEntry}
			for {
				if _, isLeader := rf.GetState(); !isLeader {
					return
				}
				if rf.nextIndex[server] != entries[0].Index {
					rf.mu.Lock()
					for i := entries[0].Index - 1; i >= rf.nextIndex[server]; i-- {
						entries = append([]*LogEntry{rf.logs[i - 1]}, entries...)
					}
					rf.mu.Unlock()
				}
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = rf.logs[rf.nextIndex[server] - 1 - 1].Term
				}
				appendEntriesArgs := AppendEntriesArgs{
					rf.currentTerm,
					rf.me, 
					entries,
					prevLogIndex,
					prevLogTerm,
					rf.committedIndex, 
				}
				fmt.Printf("to server %d %+v index %d\n", server, appendEntriesArgs, index)

				appendEntriesReply := AppendEntriesReply{}
				ok := rf.SendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)

				if !ok {
					fmt.Printf("!!!!!%dto server %d failed!!!!!\n", rf.me, server)					
					continue
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if appendEntriesReply.Success {
					locker.Lock()
					replicateCount++
					cond.Signal()
					locker.Unlock()
					rf.nextIndex[server]++
					if newEntry.Index > rf.matchIndex[server] {
						rf.matchIndex[server] = newEntry.Index
					}
					fmt.Printf("server %d append %d success reply, replicate count %d \n", server, index, replicateCount)
					return
				}

				rf.nextIndex[server]--

			}
		}(server)
	}

	go func(cond *sync.Cond) {
		locker.Lock()
		for !(replicateCount > len(rf.peers) / 2) {
			cond.Wait()
		}
		rf.mu.Lock()
		if rf.currentTerm != rf.logs[index - 1].Term {
			return
		}
		if index > rf.committedIndex {
			for i := rf.committedIndex + 1; i <= index; i++ {
				rf.applyCh <- ApplyMsg{true, rf.logs[i - 1].Command, i}
			}
			rf.committedIndex = index
			fmt.Printf("++++Commit index %d++++\n", index)
		}
		rf.mu.Unlock()
		locker.Unlock()
	}(cond)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killChan <- true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// Initial state
	rf.mu.Lock()
	
	rf.applyCh = applyCh

	rf.votedFor = -1
	rf.voteChan = make(chan int)
	rf.closeVoteChan = make(chan bool, 1)
	rf.onEmptyElectionTerm = false
	
	rf.status = follower
	rf.currentTerm = 0
	rf.killChan = make(chan bool, 1)

	rf.logs = []*LogEntry{}
	rf.committedIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	// Start timing
	rf.clockInterval = time.Duration(rand.Intn(150) + 200 + (rf.me*100)%150)
	rf.heartBeatClock = time.NewTimer(rf.clockInterval*time.Millisecond)
	rf.mu.Unlock()
	go func() {
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
						rf.clockInterval = time.Duration(rand.Intn(150) + 200 + (rf.me*100)%150)
						rf.mu.Unlock()

						if rf.onEmptyElectionTerm {
							continue
						}
						rf.voteChan = make(chan int)
						rf.StartElection()
					} else {
						// else send heartbeat
						rf.SendHeartbeat()
					}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) SetHeatBeatClock() {
	rf.heartBeatClock.Reset(rf.clockInterval*time.Millisecond)
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	newTerm := rf.currentTerm + 1
	rf.status = candidate
	rf.votedFor = rf.me
	rf.mu.Unlock()

	closeSendVoteChan := make(chan bool, 1)
	isVoteClose := false

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int, closeSendVoteChan chan bool, isVoteClose *bool) {
			lastLogIndex := len(rf.logs)
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.logs[lastLogIndex - 1].Term
			}

			requestVoteArgs := RequestVoteArgs{newTerm, rf.me, lastLogIndex, lastLogTerm}
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if ok {
				if requestVoteReply.VoteGranted && !*isVoteClose{
					select {
						case <- closeSendVoteChan:
							*isVoteClose = true
							close(rf.voteChan)
							return
						case rf.voteChan <- 1:
							return
					}
				} else if requestVoteReply.Term > rf.currentTerm {
					rf.BackToFollower(requestVoteReply.Term)
					return
				}
			}
		}(server, closeSendVoteChan, &isVoteClose)
	}

	go func(closeSendVoteChan chan bool, newTerm int) {
		voteCount := 1
		for {
			select {
				case _, ok := <- rf.voteChan:
					if !ok {
						break
					}
					voteCount++
					if voteCount > len(rf.peers) / 2 {
						fmt.Printf("++__+++Server %d is leader+++__+++\n", rf.me)
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
						closeSendVoteChan <- true
						rf.SendHeartbeat()
						return
					}
				case <- rf.closeVoteChan:
					closeSendVoteChan <- true
					return
				default:
					if rf.status != candidate {
						break
					}
					continue
			}	
		}	
	}(closeSendVoteChan, newTerm)
}

func (rf *Raft) SendHeartbeat() {
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
			if !reply.Success {
				rf.BackToFollower(reply.Term)
			}
		}(currentTerm, server)
	}	
}

// AppendEntry logic

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	Entries []*LogEntry
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.logs))
		fmt.Printf("server %d committedIndex %d\n", rf.me, rf.committedIndex)
	}

	if rf.committedIndex > rf.lastApplied && rf.IsLogConsecutive(){
		for i := rf.lastApplied + 1; i <= rf.committedIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.logs[i - 1].Command, rf.logs[i - 1].Index}
		}
		rf.lastApplied = rf.committedIndex
	}

	if len(args.Entries) == 0 {
		// dealing with heatbeat here
		reply.Success = true
		rf.BackToFollower(args.Term)
		return
	}
	// fmt.Printf("==========Server %d Append Args %+v===============\n", rf.me, args)
	if args.PrevLogIndex <= 0 || rf.logs[args.PrevLogIndex - 1].Term == args.PrevLogTerm {
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			fmt.Printf("Server %d append entries %+v\n", rf.me, args.Entries[i])
			entry := args.Entries[i]
			if entry.Index <= len(rf.logs) {
				if rf.logs[entry.Index - 1].Term != entry.Term {
					rf.logs = rf.logs[:entry.Index - 1]
				} else {
					continue
				}
			}
			// 	newSlot := entry.Index - len(rf.logs)
			// 	for i := 0; i < newSlot; i++ {
			// 		rf.logs = append(rf.logs, nil)	
			// 	}
			// }
			// rf.logs[entry.Index - 1] = entry
			rf.logs = append(rf.logs, entry)
		}
	} else {
		reply.Success = false
	}
	

}

func (rf *Raft) BackToFollower(newTerm int) {
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
}


func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) IsLogConsecutive() bool {
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i] == nil {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a >= b {
		return b
	}
	return a
}
