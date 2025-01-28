package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	currentTerm int
	state       State
	votedFor    int
	logs        []LogEntry

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg // from Make()
	killCh  chan bool     // for Kill()

	voteCh   chan struct{}
	appendCh chan struct{}
}

// Your data here (4A, 4B).
// Look at the paper's Figure 2 for a description of what
// state a Raft server must maintain.

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// Your code here (4B).
// Example:
// w := new(bytes.Buffer)
// e := labgob.NewEncoder(w)
// e.Encode(rf.xxx)
// e.Encode(rf.yyy)
// data := w.Bytes()
// rf.persister.SaveRaftState(data)

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("error reading encoded data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = log
	}
}

// Your code here (4B).
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

// retrieves index of last log entry
// must be holding lock
func (rf *Raft) RetrieveLogIndex() int {
	return len(rf.logs) - 1
}

// retrieves term of last log entry
// must already be holding lock
func (rf *Raft) RetrieveLatestTerm() int {
	FinalTerm := rf.logs[rf.RetrieveLogIndex()].Term
	return FinalTerm
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//log.Printf("[%d] RequestVote() called with args: %+v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (args.Term < rf.currentTerm) || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	} else if args.LastLogTerm < rf.RetrieveLatestTerm() || (args.LastLogTerm == rf.RetrieveLatestTerm() && args.LastLogIndex < rf.RetrieveLogIndex()) {
		return
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower
		rf.persist()
		select {
		case rf.voteCh <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	//log.Printf(" %d became leader", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.RetrieveLogIndex() + 1
	}
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	//log.Printf(" %d became candidate", rf.me)
	rf.persist()
	go rf.broadcastVoteReq()
}

func (rf *Raft) becomeFollower(term int) {
	//log.Printf(" %d became follower", rf.me)
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	term, isLeader = rf.GetState()

	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.persist()
		//log.Printf("Creating new log entry: Index=%d, Term=%d, Command=%v", index, term, command)
		rf.mu.Unlock()

	}
	return index, term, isLeader

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("[%d] AppendEntries() called with args: %+v", rf.me, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("server %d term %d receive AE args %v local log %v", rf.me, rf.currentTerm, args, rf.logs)
	select {
	case rf.appendCh <- struct{}{}:
	default:
	}
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.ConflictIndex, reply.ConflictTerm = 0, -1
	sizeOfLog := len(rf.logs)
	//log.Printf("append entries log sent to server %d log is %v for ", rf.me, args.Entries)
	// Check for log entry at PrevLogIndex.
	if args.PrevLogIndex >= sizeOfLog {
		log.Printf("[%d] AppendEntries: PrevLogIndex=%d is out of range. Log length=%d",
			rf.me, args.PrevLogIndex, len(rf.logs))
		reply.ConflictIndex = sizeOfLog
		return
	}
	// if log inconsistentcy found Iterate to find the first index in the conflicting term.
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		log.Printf("[%d] AppendEntries: Log inconsistency found at index %d, term=%d (expected=%d)",
			rf.me, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)

		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}

	// deleting conflicting entries and appending new ones
	indexToUpdate := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if indexToUpdate < sizeOfLog {
			if rf.logs[indexToUpdate-1].Term != entry.Term {
				rf.logs = rf.logs[:indexToUpdate]
			}
		}
		rf.logs = append(rf.logs, entry)
		indexToUpdate++
	}

	log.Printf("[Node %d] Evaluating commit index update: Current commitIndex = %d, LeaderCommit = %d", rf.me, rf.commitIndex, args.LeaderCommit)
	// Update commitIndex if necessary.
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := max(0, indexToUpdate-1) //cant be negative index
		newCommitIndex := min(args.LeaderCommit, lastNewEntryIndex)

		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			log.Printf("changing commit index to %d", rf.commitIndex)
			//apply to state machine
			rf.pushCommit()
		}
	}
	rf.persist()
	reply.Success = true
}
func (rf *Raft) broadcastHeartbeat() {
	for server := range rf.peers {
		if server != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.logs) {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}

				entries := rf.getEntriesAfterIndex(prevLogIndex)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: rf.commitIndex,
					Entries:      entries,
				}
				//log.Printf("Leader %d log state: %v", rf.me, rf.logs)
				//log.Printf("args previous log index is %d and prev log Term is %d, length of entries is %d for peer: %d", args.PrevLogIndex, args.PrevLogTerm, len(entries), peer)
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, reply)

				rf.mu.Lock()
				if !ok {

					rf.mu.Unlock()
					return
				}

				if rf.state != Leader || rf.currentTerm != args.Term {
					//log.Printf("state is not leader or current term is not args term")
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					//log.Printf("[%d] broadcastVoteReq(): Received term greater than current term, becoming follower", rf.me)
					rf.becomeFollower(reply.Term)

					rf.mu.Unlock()
					return
				}

				// if did not successfully append entry, decrement next index and try again
				if !reply.Success {

					log.Printf(" decrementing next index for peer %d", peer)
					rf.decrementNextIndex(peer, *reply)
				}

				if reply.Success {
					// Calculate the current match index based on the last log entry sent
					curMatchIndex := args.PrevLogIndex + len(args.Entries)

					// Update the match index if the current match index is higher
					if curMatchIndex > rf.matchIndex[peer] {
						rf.matchIndex[peer] = curMatchIndex
						//log.Printf("[%d] Updated matchIndex for follower %d to %d", rf.me, peer, curMatchIndex)
					}

					// Update the next index for the follower
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					//log.Printf("[%d] Updated nextIndex for follower %d to %d", rf.me, peer, rf.nextIndex[peer])
					//rf.mu.Unlock()

					//return
					// Find the highest log index replicated on a majority of servers
					majorityIndex := len(rf.peers) / 2
					for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= i {
								count++
							}
						}
						if count > majorityIndex {
							// Check if the log entries up to index i are consistent across the majority
							consistent := true
							for j := range rf.peers {
								if j != rf.me && rf.matchIndex[j] >= i {
									if rf.logs[i].Term != rf.logs[rf.matchIndex[j]].Term {
										consistent = false
										break
									}
								}
							}
							if consistent {
								rf.commitIndex = i
								log.Printf("[Node %d] Updated commitIndex to %d", rf.me, rf.commitIndex)
								go rf.pushCommit()
							}
							break
						}
					}
				}
				rf.mu.Unlock()
			}(server)
		}
	}
}

// helper function for heartbeat to decrement Next Index
func (rf *Raft) decrementNextIndex(peer int, reply AppendEntriesReply) {
	index := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		index = rf.getLogIndexByTerm(reply.ConflictTerm, index) + 1
	}
	rf.nextIndex[peer] = min(len(rf.logs), index)
}

// helper function for previous func
func (rf *Raft) getLogIndexByTerm(term int, startIndex int) int {
	for i := startIndex; i >= 0; i-- {
		if i < len(rf.logs) && rf.logs[i].Term == term {
			return i
		}
	}
	return -1
}

// helper func to get entries after index
func (rf *Raft) getEntriesAfterIndex(index int) []LogEntry {
	if index+1 >= len(rf.logs) {
		return []LogEntry{}
	}
	entries := make([]LogEntry, len(rf.logs)-index-1)
	copy(entries, rf.logs[index+1:])
	return entries
}

func (rf *Raft) broadcastVoteReq() {

	//log.Printf("server%d is broadcasting vote Request", rf.me)
	rf.mu.Lock()
	//log.Printf("got here 0")
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.RetrieveLogIndex(),
		rf.RetrieveLatestTerm(),
	}
	//log.Printf("got here 1")
	rf.mu.Unlock()

	votes := int32(1)
	//log.Printf("got here 2")
	for server := range rf.peers {
		if server != rf.me {

			go func(server int) {
				reply := &RequestVoteReply{}

				if ok := rf.sendRequestVote(server, &args, reply); ok {
					//log.Printf("got here now")
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						//log.Printf("[%d] broadcastVoteReq(): Received term greater than current term, becoming follower", rf.me)
						rf.becomeFollower(reply.Term)
						return
					}
					if rf.state != Candidate || rf.currentTerm != args.Term {
						return
					}
					if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
					if atomic.LoadInt32(&votes) == int32(len(rf.peers)/2+1) {
						select {
						case rf.voteCh <- struct{}{}:
						default:
						}
						rf.becomeLeader()
						//log.Printf("[%d] broadcastVoteReq(): Received majority votes, becoming leader", rf.me)
						rf.broadcastHeartbeat()

					}
				}
			}(server)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf("[%d] sendRequestVote() called for server %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) pushCommit() {
	// Apply committed entries to the state machine
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied = i
	}
	log.Printf("Committing log entries from index %d to %d", rf.lastApplied+1, rf.commitIndex)
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//log.Printf("[%d] ticker() started", rf.me)
	for !rf.killed() {
		select {
		case <-rf.killCh:
			return
		default:
		}

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		electionTimeout := time.Duration(rand.Intn(200)+400) * time.Millisecond
		heartbeatTime := time.Duration(100) * time.Millisecond
		switch state {

		case Leader:
			time.Sleep(heartbeatTime)
			rf.broadcastHeartbeat()

		case Follower, Candidate:
			rf.FollowerorCandidate(electionTimeout)

		}
		// Wait for a fraction of the election timeout before the next iteration
		time.Sleep(electionTimeout / 10)
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
}

func (rf *Raft) FollowerorCandidate(electionTimeout time.Duration) {
	select {
	case <-rf.voteCh:
	case <-rf.appendCh:
	case <-time.After(electionTimeout):
		rf.mu.Lock()
		rf.becomeCandidate()
		rf.mu.Unlock()
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	// rf.logs[0] = LogEntry{}

	rf.voteCh = make(chan struct{}, 1)

	rf.appendCh = make(chan struct{}, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	//rf.ElectionTimer = time.NewTimer(getElectionTimeout())
	rf.applyCh = applyCh

	rf.killCh = make(chan bool, 1)

	rf.voteCh = make(chan struct{}, 1)
	rf.appendCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
