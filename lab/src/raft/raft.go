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
import "reflect"



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

type Status string

const (
	follower Status = "follower"
	candidate Status = "candidate"
	leader Status = "leader"
)

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

	// universal states for all servers
	status Status
	currentTerm int
	votedFor int
	log []*LogEntry

	// volatile state for all servers
	committedIndex int
	lastApplied int

	// volatile state on leader
	nextIndex []int
	matchIndex []int

	// leader election
	clock *time.Timer
	clockInterval time.Duration
	// since we wait for 1 whole clock interval before every
	// round of leader of election, this onEmptyElectionTerm
	// will indicate whether current clock interval should be waiting
	// or not
	onEmptyElectionTerm bool
	// channel used for vote control
	voteChan chan int
	closeVoteChan chan bool

	// other
	applyCh chan ApplyMsg
	killChan chan bool

	attributesLocks map[string]*sync.Mutex

}

func (rf *Raft) GenerateLocks() {
	noLocksFields := make(map[string]struct{})
	noLocksFields["mu"] = struct{}{}
	noLocksFields["peers"] = struct{}{}
	noLocksFields["persister"] = struct{}{}
	noLocksFields["me"] = struct{}{}
	noLocksFields["attributesLocks"] = struct{}{}

	fields := reflect.ValueOf(rf).Elem()

	for i := 0; i < fields.NumField(); i++ {
		field := fields.Type().Field(i)
		_, exists := noLocksFields[field.Name]
		if exists {
			continue
		}
		rf.attributesLocks[field.Name] = &sync.Mutex{}
	}
}

func (rf *Raft) RequestLock(fields []string) {
	for _, field := range fields {
		rf.attributesLocks[field].Lock()
	}
}

func (rf *Raft) ReleaseLock(fields []string) {
	for _, field := range fields {
		rf.attributesLocks[field].Unlock()
	}	
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.RequestLock([]string{"status", "currentTerm"})
	term = rf.currentTerm
	isleader = (rf.status == leader)
	rf.ReleaseLock([]string{"status", "currentTerm"})
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


	return index, term, isLeader
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	
	rf.applyCh = applyCh

	rf.votedFor = -1
	rf.onEmptyElectionTerm = false
	rf.closeVoteChan = make(chan bool, 1)
	
	rf.status = follower
	rf.currentTerm = 0
	rf.killChan = make(chan bool, 1)

	rf.log = []*LogEntry{}
	rf.committedIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	// Start timing
	rf.clockInterval = time.Duration(rand.Intn(150) + 200 + (rf.me*100)%150)
	rf.clock = time.NewTimer(rf.clockInterval*time.Millisecond)

	rf.attributesLocks = make(map[string]*sync.Mutex)
	rf.mu.Unlock()

	rf.GenerateLocks()

	go rf.LeaderElection()

	return rf
}
