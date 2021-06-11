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

import (
	"bytes"
	"io/ioutil"
	"labgob"
	"labrpc"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

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

// Log reprents a log which will be placed in the logs array.
type Log struct {
	Term    int
	Index   int
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
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Persistent state.
	currentTerm       int
	votedFor          int
	log               []Log
	leaderID          int
	hearbeatTimeStamp int64

	// Volatile state.
	commitIndex int
	applyIndex  int

	// Volatie leader states.
	nextIndex  []int // next index to be sent.
	matchIndex []int // index of highest log entry known to be replicated.

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Me: %d, Term: %d, Leader: %d", rf.me, rf.currentTerm, rf.leaderID)

	return rf.currentTerm, rf.leaderID == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.leaderID)
	log.Printf("Encoder: %d, %d, %v, %d, %d", rf.currentTerm, rf.votedFor, rf.log, rf.leaderID, rf.me)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var raftLog []Log
	var leaderID int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&raftLog) != nil ||
		d.Decode(&leaderID) != nil {
		log.Fatal("Failed while decoding.")
	} else {
		log.Printf("Decoder: %d, %d, %v, %d, %d", currentTerm, votedFor, raftLog, leaderID, rf.me)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = raftLog
		rf.leaderID = leaderID
	}
}

//
// Append Entries Request.
//
type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	CommitIndex  int
}

//
// Append Entries Reply.
//
type AppendEntriesReply struct {
	Term                   int
	Success                bool
	PeerLogLen             int
	PeerNextTermOnMismatch int
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Append Entries received from leader: %d to me: %d. CurrentTerm: %d, ReceivedTerm: %d", args.LeaderID, rf.me, rf.currentTerm, args.Term)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	logLen := len(rf.log)
	rf.leaderID = args.LeaderID
	reply.Term = rf.currentTerm
	reply.PeerLogLen = logLen
	rf.hearbeatTimeStamp = time.Now().UTC().UnixNano()
	rf.persist()

	if (logLen == 0 && args.PrevLogIndex == 0) || args.PrevLogTerm == 0 {
		log.Printf("The logs will be appended.")
	} else if logLen < args.PrevLogIndex {
		log.Printf("The logs will not be appended: loglen is less the prevlogindex.")
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		log.Printf("The logs will not be appended: the prevlogindex term does not match.")
		index := args.PrevLogIndex - 1
		prevTerm := -1
		for index >= 0 {
			if rf.log[args.PrevLogIndex-1].Term == rf.log[index].Term {
				index--
				continue
			}

			prevTerm = rf.log[index].Term
			break
		}

		reply.PeerNextTermOnMismatch = prevTerm
		reply.Success = false
		return
	}

	rf.log = rf.log[:args.PrevLogIndex]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	rf.persist()

	if args.CommitIndex > rf.commitIndex {
		if args.CommitIndex < len(rf.log) {
			rf.commitIndex = args.CommitIndex
		} else {
			rf.commitIndex = len(rf.log)
		}

		log.Printf("Higher commit index received %d, me: %d.", len(args.Entries), rf.me)
	}

	return
}

//
// Send AppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Request Vote TermId: %d, CandidateId: %d, Me: %d, MeTerm: %d.", args.Term, args.CandidateID, rf.me, rf.currentTerm)
	if rf.currentTerm > args.Term ||
		(rf.votedFor == rf.me && rf.currentTerm == args.Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	logLen := len(rf.log)

	if logLen != 0 &&
		(rf.log[logLen-1].Term > args.LastLogTerm ||
			(rf.log[logLen-1].Term == args.LastLogTerm &&
				logLen > args.LastLogIndex)) {
		log.Printf(
			"latest log details me - Term: %d, LogLen: %d. RequestVote - Term: %d, LastLogIndex: %d",
			rf.log[logLen-1].Term,
			logLen,
			args.LastLogTerm,
			args.LastLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	log.Printf("Request Vote Granted TermId: %d, CandidateId: %d, Me: %d, MeTerm: %d.", args.Term, args.CandidateID, rf.me, rf.currentTerm)
	rf.currentTerm = args.Term
	rf.leaderID = -1
	rf.votedFor = args.CandidateID
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.hearbeatTimeStamp = time.Now().UTC().UnixNano()
	rf.persist()
	return
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
// the service using Raft (e.g. a k/v server) wants to `start`
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
	// Your code here (2B).
	log.Printf("Command Received %d", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderID == rf.me {

		log.Printf("Command Processed as I am leader %d with current log size: %d", rf.me, len(rf.log))
		rf.log = append(
			rf.log,
			Log{
				Term:    rf.currentTerm,
				Index:   len(rf.log) + 1,
				Command: command})
		rf.matchIndex[rf.me] = len(rf.log)
		rf.persist()
		go rf.SendAppendEntriesToPeer()
	} else {
		log.Printf("Command not processed as leader %d", rf.leaderID)
	}

	return len(rf.log), rf.currentTerm, rf.leaderID == rf.me
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.leaderID = -1
	rf.applyCh = applyCh
	rf.log = []Log{}
	rf.commitIndex = 0
	rf.applyIndex = 0

	log.SetOutput(ioutil.Discard) // Disable Logging.
	log.Printf("Total Peers: %d Me: %d", len(rf.peers), rf.me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	if rf.leaderID == rf.me {
		rf.nextIndex = make([]int, len(rf.peers))
		for index, _ := range rf.nextIndex {
			rf.nextIndex[index] = len(rf.log) + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = len(rf.log)
	}
	rf.mu.Unlock()
	// Start GO ROUTINE.
	// Periodic routine, with the next timer ser to Election Timeout which
	// is random value between 150ms and 300ms seconds.
	// Upon expiry of the routine, request for vote.
	// If hearbeat is received by the leader, reset the periodic routine.
	// Else transition to candidate.
	// with the next timer set to Election Timeout.

	go rf.Run()

	return rf
}

//
func (rf *Raft) Run() {
	rand.Seed(time.Now().UnixNano())
	max := 300
	min := 150
	hearbeatInterval := 80

	var electionTimeout int
	var leader bool
	isFollowerTimedOut := func() bool {
		return time.Now().UTC().UnixNano()-rf.hearbeatTimeStamp >
			(time.Duration(max) * time.Millisecond).Nanoseconds()
	}

	go rf.ApplyLongRunning()
	go rf.CommitLongRuning()

	for {
		rf.mu.Lock()
		// If leader, send appendArgs.
		// else if the
		//     hearbeat timestamp is older than the timeout
		//     start election else sleep and assume leadership.
		leader = rf.leaderID == rf.me
		if leader {
			electionTimeout = hearbeatInterval
		} else {
			electionTimeout = rand.Intn(max-min) + min
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		go rf.ExecuteServer(isFollowerTimedOut)
	}
}

//
func (rf *Raft) ApplyLongRunning() {
	for {
		rf.Apply()
		time.Sleep(200 * time.Millisecond)
	}
}

//
func (rf *Raft) CommitLongRuning() {
	for {
		rf.Commit()
		time.Sleep(200 * time.Millisecond)
	}
}

//
func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderID != rf.me {
		return
	}

	// What is the minumum match index for the majority?
	// 1 2 2  - 2
	//
	commitIndex := math.MaxInt32
	var sortedIndex []int = make([]int, len(rf.matchIndex))
	copy(sortedIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(sortedIndex)))

	majority := (len(rf.peers) + 1) / 2
	for index, num := range sortedIndex {
		if num < commitIndex {
			commitIndex = num
		}

		if index+1 >= majority {
			log.Printf("Reached majority index: %d, majority: %d, value: %d, me:%d", index+1, majority, commitIndex, rf.me)
			log.Printf("%v, ordered: %v", sortedIndex, rf.matchIndex)
			break
		}
	}

	if commitIndex == rf.commitIndex {
		log.Printf("No apply action required as commitindex %d me: %d.", commitIndex, rf.me)
		return
	} else if commitIndex < rf.commitIndex {
		log.Printf("The min match index cannot be less than leader commit index.\n")
	}

	log.Printf("The commitIndex: %d, me: %d.\n", commitIndex, rf.me)
	if commitIndex >= 1 && rf.log[commitIndex-1].Term == rf.currentTerm {
		rf.commitIndex = commitIndex
	}
}

//
func (rf *Raft) Apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Apply thread me: %d.", rf.me)
	condition := sync.NewCond(&rf.mu)

	for i := rf.applyIndex + 1; i <= rf.commitIndex; i++ {
		go func() {
			rf.mu.Lock()
			if i-1 >= len(rf.log) {
				return
			}

			defer rf.mu.Unlock()
			log.Printf("Logs applied %d, me:%d.", i, rf.me)
			logEntry := rf.log[i-1]
			applyMsg := ApplyMsg{CommandValid: true, Command: logEntry.Command, CommandIndex: logEntry.Index}
			rf.applyCh <- applyMsg
			condition.Signal()
		}()

		condition.Wait()
		rf.applyIndex++
	}
}

// ExecuteServer Raft Server ...
func (rf *Raft) ExecuteServer(isFollowerTimedOut func() bool) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderID == rf.me {
		log.Printf("I am the leader %d", rf.me)
		go rf.SendAppendEntriesToPeer()
		return rf.leaderID == rf.me
	}

	if !isFollowerTimedOut() {
		return false
	}

	log.Printf("Going for election: %d\n.", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	votes := make(chan bool)

	lastLogTerm := 0
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	rf.persist()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		reqestVoteMessage := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: len(rf.log),
			LastLogTerm:  lastLogTerm}
		responeVoteReply := RequestVoteReply{}
		log.Printf("Sending request vote %d->%d\n", rf.me, index)

		go func(peerId int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.sendRequestVote(peerId, &reqestVoteMessage, &responeVoteReply)
			votes <- responeVoteReply.VoteGranted
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				log.Printf("Term received: %d, Current Term: %d, Me: %d\n", responeVoteReply.Term, rf.currentTerm, rf.me)
				if !responeVoteReply.VoteGranted && responeVoteReply.Term > rf.currentTerm {
					if responeVoteReply.Term > rf.currentTerm {
						log.Printf("Resetting the term as part of response vote - Term: %d, Me: %d", responeVoteReply.Term, rf.me)
						rf.currentTerm = responeVoteReply.Term
						rf.persist()
					}
				}
			}
		}(index, &reqestVoteMessage, &responeVoteReply)
	}

	rf.mu.Unlock()
	log.Printf("Counting Votes Me: %d.\n", rf.me)
	var voteCount int
	length := len(rf.peers)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		voteGranted := <-votes

		if voteGranted {
			voteCount++
		}

		if voteCount+1 >= (length+1)/2 {
			log.Printf("I am the new leader %d", rf.me)
			rf.mu.Lock()
			rf.leaderID = rf.me
			rf.nextIndex = make([]int, len(rf.peers))
			for index, _ := range rf.nextIndex {
				rf.nextIndex[index] = len(rf.log) + 1
			}
			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = len(rf.log)
			go rf.SendAppendEntriesToPeer()
			rf.persist()
			return true
		}

	}

	rf.mu.Lock()
	log.Printf("I lost the election %d", rf.me)
	return false
}

// SendAppendEntriesToPeer ...
func (rf *Raft) SendAppendEntriesToPeer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderID == rf.me {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}

			go func(peerId int) {
				// For peerId, check the last synced log.
				// Send the prevLogIndex, prevLogTerm, entries, commitIndex.
				// send all logs from last synced log to the peer.
				// if response is received from majority, commit the log, else drop it.
				// should it retry sending the log which failed?
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.leaderID != rf.me {
					return
				}

				lastLogIndex := len(rf.log)
				var entries []Log
				if lastLogIndex >= rf.nextIndex[peerId] {
					entries = make([]Log, len(rf.log)-rf.nextIndex[peerId]+1)
					copy(entries, rf.log[rf.nextIndex[peerId]-1:])
				}

				var prevLogTerm int
				if (rf.nextIndex[peerId] - 1) >= 1 {
					prevLogTerm = rf.log[rf.nextIndex[peerId]-1-1].Term
				}

				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[peerId] - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					CommitIndex:  rf.commitIndex,
				}

				appendEntriesReply := AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(peerId, &appendEntriesArgs, &appendEntriesReply)
				rf.mu.Lock()

				if rf.leaderID != rf.me || rf.currentTerm != appendEntriesArgs.Term {
					return
				}

				if !ok {
					log.Printf("The AppendEntry RPC failed peerId: %d, me: %d.", peerId, rf.me)
				} else if appendEntriesReply.Term > rf.currentTerm {
					rf.leaderID = -1
					rf.currentTerm = appendEntriesReply.Term
					log.Printf("I am no longer a leader %d.", rf.me)
				} else if !appendEntriesReply.Success {
					if rf.nextIndex[peerId] > appendEntriesReply.PeerLogLen {
						if appendEntriesReply.PeerLogLen >= 1 {
							rf.nextIndex[peerId] = appendEntriesReply.PeerLogLen
						} else if appendEntriesReply.PeerLogLen == 0 {
							rf.nextIndex[peerId] = 1
						}

						log.Printf("Append entries reset the nextindex %d.", appendEntriesReply.PeerLogLen)
					} else if rf.nextIndex[peerId] > 1 {
						// Optimize the decrement, to directly jump into the next term known by the peer.
						if appendEntriesReply.PeerNextTermOnMismatch != -1 {
							index := rf.nextIndex[peerId] - 1
							for index >= 1 && rf.log[index-1].Term != appendEntriesReply.PeerNextTermOnMismatch {
								index--
							}
							log.Printf("previous index %d, new index %d", rf.nextIndex[peerId], index)
							rf.nextIndex[peerId] = index + 1
						} else {
							rf.nextIndex[peerId]--
							log.Printf("index decremented %d", rf.nextIndex[peerId])
						}
					}
				} else if appendEntriesReply.Success {
					log.Printf(
						"Success received append enetries me: %d, lastMatchIndex %d, peer: %d.",
						rf.me,
						lastLogIndex,
						peerId)
					rf.nextIndex[peerId] = lastLogIndex + 1
					rf.matchIndex[peerId] = lastLogIndex
				}

				rf.persist()
			}(index)
		}
	}
}
