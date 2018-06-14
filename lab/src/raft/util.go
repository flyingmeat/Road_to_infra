package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// getLastLog returns the last local log entry.
func (rf *Raft) getLastLog() *LogEntry {
	logSize := len(rf.log)
	if (logSize == 0) {
		return nil
	}
	return &rf.log[logSize - 1]
}
