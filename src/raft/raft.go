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
	"io/ioutil"
	"labrpc"
	"log"
	"math/rand"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	currentTerm       int
	votedFor          int
	hearbeatTimeStamp int64
	leaderID          int
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
// Append Entries Request.
//
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

//
// Append Entries Reply.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	log.Printf("Append Entries received from leader %d to me %d.", args.LeaderID, rf.me)
	rf.leaderID = args.LeaderID
	rf.hearbeatTimeStamp = time.Now().UTC().UnixNano()
	reply.Term = rf.currentTerm
	reply.Success = true

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
	Term        int
	CandidateID int
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
		reply = &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}

	rf.currentTerm = args.Term
	rf.leaderID = -1
	rf.votedFor = args.CandidateID
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.hearbeatTimeStamp = time.Now().UTC().UnixNano()
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

	log.SetOutput(ioutil.Discard) // Disable Logging.
	log.Printf("Total Peers: %d Me: %d", len(rf.peers), rf.me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
	hearbeatInterval := 100

	var electionTimeout int
	var leader bool
	isFollowerTimedOut := func() bool {
		return time.Now().UTC().UnixNano()-rf.hearbeatTimeStamp >
			(time.Duration(min) * time.Millisecond).Nanoseconds()
	}

	for {
		// If leader, send appendArgs.
		// else if the
		//     hearbeat timestamp is older than the timeout
		//     start election else sleep and assume leadership.
		if leader {
			electionTimeout = hearbeatInterval
		} else {
			electionTimeout = rand.Intn(max-min) + min
		}

		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		leader = rf.ExecuteServer(isFollowerTimedOut)
	}
}

// ExecuteServer Raft Server ...
func (rf *Raft) ExecuteServer(isFollowerTimedOut func() bool) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderID == rf.me {
		log.Printf("I am the leader %d", rf.me)
		return rf.SendAppendEntriesToPeer()
	}

	if !isFollowerTimedOut() {
		return false
	}

	log.Printf("Going for election: %d\n.", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	votes := make(chan bool)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(peerId int) {
			reqestVoteMessage := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me}
			responeVoteReply := RequestVoteReply{}
			log.Printf("Sending request vote %d->%d\n", rf.me, peerId)
			rf.sendRequestVote(peerId, &reqestVoteMessage, &responeVoteReply)
			votes <- responeVoteReply.VoteGranted
		}(index)
	}

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
			rf.leaderID = rf.me
			return rf.SendAppendEntriesToPeer()
		}

	}

	log.Printf("I lost the election %d", rf.me)
	return false
}

// SendAppendEntriesToPeer ...
func (rf *Raft) SendAppendEntriesToPeer() bool {
	var wg sync.WaitGroup

	if rf.leaderID == rf.me {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}

			wg.Add(1)

			go func(peerId int) {
				appendEntriesArgs := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}
				appendEntriesReply := AppendEntriesReply{}
				rf.sendAppendEntries(peerId, &appendEntriesArgs, &appendEntriesReply)
				if appendEntriesReply.Term > rf.currentTerm {
					rf.leaderID = -1
					rf.currentTerm = appendEntriesReply.Term
					log.Printf("I am no longer a leader %d.", rf.me)
				}

				wg.Done()
			}(index)
		}
	}

	wg.Wait()
	return rf.leaderID == rf.me
}
