package raft

import "sync"

func (rf *Raft) initLocks() {
	lockerNames := []string{
		"currentTerm",
		"votedFor",
		"log",
		"commitIndex",
		"lastApplied",
		"nextIndex",
		"matchIndex",
		"state",
		"lastHeartBeat",
		"leader",
	}
	rf.mus = map[string]*sync.Mutex{}
	for _, name := range lockerNames {
		rf.mus[name] = &sync.Mutex{}
	}
}

func (rf *Raft) acquireLocks(names ...string) {
	for _, name := range names {
		rf.mus[name].Lock()
	}
}

func (rf *Raft) releaseLocks(names ...string) {
	for _, name := range names {
		rf.mus[name].Unlock()
	}
}
