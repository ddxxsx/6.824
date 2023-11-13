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
	"6.5840/Log"
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Command interface{}
}

const HEARTBEATPERIOD float64 = 100
const APPENDCHECKPERIOD int = 10
const TIMEOUTPERIOD int = 350

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	TermId       int //term number
	LastVoted    int // last voted id
	TimeStamp    time.Time
	Log          []LogEntry
	Role         RoleType
	NextIndex    []int //index of the next log entry to send to that server
	LastCommitId int
	LastApplyId  int
	ApplyCh      chan ApplyMsg
	LastIncludedIndex int
	LastIncludedTerm int
}
type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Log         []LogEntry
	LeaderApply int
}
type AppendEntriesReply struct {
	Result bool
	Term   int
	XTerm  int //conflict log Term
	XIndex int //first log index for XTerm
	XLen   int //log not exist in follower, XTerm return -1, XLen for blank log num
}

type InstallSnapShotArgs struct {
	Term int //leader’s term
	LeaderId int // so follower can redirect clients
	LastIncludedIndex int //the snapshot replaces all entries up through and including this index
	LastIncludedTerm int  // term of lastIncludedIndex
	//Offset int // byte offset where chunk is positioned in the snapshot file
	Data[] byte// raw bytes of the snapshot chunk, starting at offset
	//Done bool //true if this is the last chunk
}
type InstallSnapShotReply struct {
	Term int
}
type RoleType int

const (
	Follower   RoleType = 0
	Leader     RoleType = 1
	Candidates RoleType = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.TermId
	isleader = rf.Role == Leader
	Log.Debug(Log.DClient, "S%d Term [%d],is leader %t", rf.me, term, isleader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	//rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.Log) != nil || e.Encode(rf.TermId) != nil || e.Encode(rf.LastVoted) != nil ||e.Encode(rf.LastIncludedIndex)!= nil||e.Encode(rf.LastIncludedTerm)!=nil{
		Log.Debug(Log.DError, "S%d encode error", rf.me)
	}

	raftstate := w.Bytes()
	//Log.Debug(Log.DInfo, "S%d save %v", rf.me, raftstate)
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	Log.Debug(Log.DPersist, "S%d read persist", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		Log.Debug(Log.DWarn, "S%d return without any state", rf.me)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var termid int
	var lastvote int
	var index int
	var term int
	if d.Decode(&logs) != nil || d.Decode(&termid) != nil || d.Decode(&lastvote) != nil ||d.Decode(&index)!= nil||d.Decode(&term)!=nil{
		Log.Debug(Log.DError, "S%d decode error", rf.me)

	} else {
		rf.Log = logs
		rf.TermId = termid
		rf.LastVoted = lastvote
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = term
		Log.Debug(Log.DInfo, "S%d LastVoted[%d],TermId[%d],log[%v]", rf.me, rf.LastVoted, rf.TermId, rf.Log)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	Log.Debug(Log.DSnap,"S%d snap called index[%d]",rf.me,index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index> len(rf.Log)+rf.LastIncludedIndex{
		Log.Debug(Log.DInfo,"S%d snap failed,index[%d] > logLen[%d]",rf.me,index, len(rf.Log))
		return
	}
	oldIndex := rf.LastIncludedIndex
	for i,j := range rf.Log{
		if i==0{
			continue
		}
		rf.LastIncludedIndex = oldIndex +i
		rf.LastIncludedTerm = j.Term
		if oldIndex+i == index{
			break
		}
	}
	temp := make([]LogEntry,1)
	temp[0].Term = rf.Log[len(rf.Log)-1].Term
	rf.Log = append(temp,rf.Log[rf.LastIncludedIndex-oldIndex+1:]...)
	//Log.Debug(Log.DSnap,"S%d snap len[%d][%d]",rf.me, len(rf.Log),oldIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.Log) != nil || e.Encode(rf.TermId) != nil || e.Encode(rf.LastVoted) != nil||e.Encode(rf.LastIncludedIndex)!= nil||e.Encode(rf.LastIncludedTerm)!=nil {
		Log.Debug(Log.DError, "S%d encode error", rf.me)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate,snapshot)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	VotedId       int
	TermId        int
	LogLen        int
	LastLogTermId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Selected bool // reply if selected
	Term     int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// not voted
	term := 0
	if len(rf.Log) != 0 {
		term = rf.Log[len(rf.Log)-1].Term
	}
	if args.TermId > rf.TermId {
		rf.TermId = args.TermId
		rf.LastVoted = -1
	}
	if rf.LastVoted == -1 {
		// Election Restrain
		if args.LastLogTermId > term {
			Log.Debug(Log.DVote, "S%d vote for S%d", rf.me, args.VotedId)
			rf.LastVoted = args.VotedId
			reply.Term = rf.TermId
			reply.Selected = true
			rf.persist()
			return
		} else {
			if args.LastLogTermId == term && rf.LastVoted == -1 && args.LogLen >= len(rf.Log)+rf.LastIncludedIndex {
				Log.Debug(Log.DVote, "S%d vote for S%d", rf.me, args.VotedId)
				rf.LastVoted = args.VotedId
				reply.Term = rf.TermId
				reply.Selected = true
				rf.persist()
				return
			}
		}
	}

	Log.Debug(Log.DVote, "S%d not vote for S%d lastvoted[%d],TermId[%d] ", rf.me, args.VotedId, rf.LastVoted, rf.TermId)
	reply.Selected = false
	reply.Term = rf.TermId

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	if rf.Role != Leader {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.Log)+rf.LastIncludedIndex
	term = rf.TermId
	isLeader = rf.Role == Leader
	if rf.Role == Leader {
		rf.Log = append(rf.Log, LogEntry{rf.TermId, command})
		rf.persist()
	}
	Log.Debug(Log.DLog, "S%d Start index[%d]term[%d]leader %t value[%v] ", rf.me, index, term, isLeader, command)

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if time.Since(rf.TimeStamp) > time.Duration(TIMEOUTPERIOD)*time.Millisecond {
			if rf.Role != Leader {
				rf.LastVoted = -1
				go rf.StartElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf * Raft) InstallSnapShot(args * InstallSnapShotArgs,reply * InstallSnapShotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log.Debug(Log.DSnap,"S%d InstallSnapShot",rf.me)
	reply.Term = -1
	if args.Term<rf.TermId{
		reply.Term = rf.TermId
		return
	}
	reply.Term = args.Term
	rf.Role = Follower
	rf.TermId = args.Term
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.LastApplyId,rf.LastCommitId = args.LastIncludedIndex,args.LastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//if (args.LastIncludedIndex>=rf.LastIncludedIndex)
	rf.Log = make([]LogEntry,1)
	rf.Log[0].Term = rf.LastIncludedTerm
	if e.Encode(rf.Log) != nil || e.Encode(rf.TermId) != nil || e.Encode(rf.LastVoted) != nil|| e.Encode(rf.LastIncludedIndex)!=nil|| e.Encode(rf.LastIncludedTerm) !=nil{
		Log.Debug(Log.DError, "S%d encode error", rf.me)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate,args.Data)
	msg := ApplyMsg{CommandValid: false,SnapshotValid:true,SnapshotIndex: rf.LastIncludedIndex,SnapshotTerm: rf.LastIncludedTerm,Snapshot: args.Data}
	rf.ApplyCh<-msg

}
func (rf *Raft) AppendTicker() {
	for {
		time.Sleep(time.Duration(APPENDCHECKPERIOD) * time.Millisecond)
		//Log.Debug(Log.DLeader,"S%d before Lock",rf.me)
		if rf.killed() || rf.Role != Leader {
			return
		}
		//if rf.LastApplyId >= len(rf.Log)-1 {
		//	continue
		//}
		//SuccessCount := 1
		//CountLock := sync.Mutex{}
		rf.mu.Lock()
		for i := range rf.peers {
			// check should send AppendEntries RPC
			if rf.NextIndex[i] == len(rf.Log)+rf.LastIncludedIndex && i != rf.me {
				//CountLock.Lock()
				//SuccessCount++
				//CountLock.Unlock()
				continue
			}
			if rf.NextIndex[i]<=rf.LastIncludedIndex && rf.me!=i{
				argc := InstallSnapShotArgs{Term: rf.TermId,LeaderId: rf.me,LastIncludedIndex: rf.LastIncludedIndex,LastIncludedTerm: rf.LastIncludedTerm,Data: rf.persister.ReadSnapshot()}
				reply :=InstallSnapShotReply{}
				Log.Debug(Log.DLog,"S%d call install",rf.me)
				go func(i int) {
					ret := rf.peers[i].Call("Raft.InstallSnapShot", &argc, &reply)
					Log.Debug(Log.DLog, "S%d install return", rf.me)
					if ret {
						if reply.Term> rf.TermId{
							rf.TermId = reply.Term
						}else if reply.Term != -1{
							rf.NextIndex[i] = rf.LastIncludedIndex +1
						}
					}
				}(i)
			} else
			{
				if i != rf.me {
					index := rf.NextIndex[i] - 1-rf.LastIncludedIndex
					term := rf.Log[index].Term
					log := rf.Log[index+1:]
					Log.Debug(Log.DLeader, "S%d index[%d],term[%d]apply[%d]", rf.me, index+rf.LastIncludedIndex, term, rf.LastCommitId)
					args := AppendEntriesArgs{LeaderId: rf.me, Term: rf.TermId, PrevLogTerm: term, PrevLogIndex: index+rf.LastIncludedIndex, Log: log, LeaderApply: rf.LastApplyId}

					go func(i int) {
						reply := AppendEntriesReply{}

						Log.Debug(Log.DInfo, "S%d send AppendEntries to [%d]", rf.me, i)
						LastIncludedIndex := rf.LastIncludedIndex
						ret := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						if !ret {
							//Log.Debug(Log.DLog, "S%d get no reply from[%d]", rf.me, i)
							return
						}
						Log.Debug(Log.DLog, "S%d get reply from[%d] %t", rf.me, i, reply.Result)
						if ret {
							if !reply.Result {
								// change NextIndex
								if reply.Term> rf.TermId{
									rf.TermId = reply.Term
									rf.LastVoted = -1
									rf.Role	 = Follower
									return
								}
								if reply.XTerm == -1 {
									Log.Debug(Log.DInfo, "S%d idx[%d]Xlen[%d]", rf.me, index, reply.XLen)
									rf.NextIndex[i] = index - reply.XLen + 1 + LastIncludedIndex
								} else {
									for j := range rf.Log {
										if rf.Log[j].Term == reply.XTerm {
											rf.NextIndex[i] = j + 1 +LastIncludedIndex
											return
										}
									}
									Log.Debug(Log.DInfo, "S%d Xidx[%d]", rf.me, reply.XIndex)

									rf.NextIndex[i] = reply.XIndex + LastIncludedIndex
								}
							} else {
								Log.Debug(Log.DLog,"S%d reply idx[%d],loglen[%d],lastInlcude[%d]",rf.me,index,len(log),LastIncludedIndex)
								rf.NextIndex[i] = index + len(log) + 1 + LastIncludedIndex
								rf.TryApplyLog(index + len(log)+LastIncludedIndex)
							}
						}
					}(i)
				}
			}


		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) TryApplyLog(idx int) {
	SuccessCount := 1
	for i := range rf.peers {
		if rf.NextIndex[i] == len(rf.Log)+ rf.LastIncludedIndex && i != rf.me {
			SuccessCount++
			continue
		}
	}
	if SuccessCount*2 > len(rf.peers) {
		rf.LastApplyId = idx
		Log.Debug(Log.DLog, "S%d LastApplyId[%d]", rf.me, rf.LastApplyId)

	}
}

func (rf *Raft) CommitTicker() {
	for !rf.killed() {
		time.Sleep(time.Duration(APPENDCHECKPERIOD) * time.Millisecond)
		rf.mu.Lock()
		LstCmtId := rf.LastCommitId
		Msgs := make([]ApplyMsg,0)
		if rf.LastApplyId > rf.LastCommitId {
			Log.Debug(Log.DTimer, "S%d apply id[%d]commit id[%d]loglen[%d]", rf.me, rf.LastApplyId, rf.LastCommitId, len(rf.Log))
			for i := rf.LastCommitId + 1; i <= rf.LastApplyId; i++ {
				msg := ApplyMsg{CommandValid: true, Command: rf.Log[i-rf.LastIncludedIndex].Command, CommandIndex: i}
				//rf.ApplyCh <- msg
				//Log.Debug(Log.DCommit, "S%d Commit index[%d] value[%v]", rf.me, i, msg.Command)
				//rf.LastCommitId = i
				Msgs = append(Msgs,msg)
			}
		}
		rf.mu.Unlock()

		for i,j :=range Msgs {
			rf.ApplyCh <- j
			Log.Debug(Log.DCommit, "S%d Commit index[%d] value[%v]", rf.me, i+LstCmtId + 1, j.Command)
			rf.LastCommitId++
		}
		if len(Msgs)!=0{
			Log.Debug(Log.DCommit, "S%d CommitDone ", rf.me)
		}
	}
}
func (rf *Raft) AppendEntries(argc *AppendEntriesArgs, reply *AppendEntriesReply) {
	//change role to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.Role = Follower
	rf.LastVoted = -1
	//Log.Debug(Log.DLog, "S%d TimeStamp change", rf.me)
	rf.TimeStamp = time.Now()

	if rf.TermId > argc.Term {
		Log.Debug(Log.DLog, "S%d AppendEntries false from [%d]", rf.me, argc.LeaderId)
		reply.Result = false
		reply.Term = rf.TermId
		return
	}
	rf.TermId = argc.Term
	if len(argc.Log) == 0 {
		Log.Debug(Log.DInfo, "S%d heartbeat", rf.me)
		//Log.Debug(Log.DInfo, "S%d log[%v],preidx[%d],prevterm[%d]", rf.me, rf.Log, argc.PrevLogIndex, argc.PrevLogTerm)

	} else {
		Log.Debug(Log.DInfo, "S%d get AppendEntries from[%d] client term [%d]", rf.me, argc.LeaderId, rf.TermId)
	}

	if len(rf.Log)-1+rf.LastIncludedIndex < argc.PrevLogIndex {
		//case log not exist
		reply.Result = false
		reply.XTerm = -1
		Log.Debug(Log.DInfo, "S%d  loglen[%d] prevlogidx[%d]", rf.me, len(rf.Log), argc.PrevLogIndex)
		reply.XLen = argc.PrevLogIndex + 1 - len(rf.Log)-rf.LastIncludedIndex
		return
	} else {
		AimTerm := rf.Log[argc.PrevLogIndex-rf.LastIncludedIndex].Term
		Log.Debug(Log.DInfo,"S%d AimTerm[%d],prevTerm[%d]",rf.me,AimTerm,argc.PrevLogTerm)
		if AimTerm == argc.PrevLogTerm {
			// matched
			Log.Debug(Log.DInfo,"S%d PrevLogIdx[%d],LstIncludeIdx[%d]",rf.me,argc.PrevLogIndex,rf.LastIncludedIndex)
			rf.Log = rf.Log[:argc.PrevLogIndex+1-rf.LastIncludedIndex]

			for i := range argc.Log {
				rf.Log = append(rf.Log, argc.Log[i])
			}

			//rf.LastApplyId = len(rf.Log) - 1
			rf.LastApplyId = argc.LeaderApply
			reply.Result = true
			return
		} else {
			for i := range rf.Log {
				if rf.Log[i].Term == AimTerm {
					reply.Result = false
					reply.XIndex = i
					reply.XTerm = AimTerm
					return
				}
			}
		}
	}

}

func (rf *Raft) SendHeartBeat() {
	for {

		if rf.killed() || rf.Role != Leader {
			return
		}
		for i := range rf.peers {
			if i != rf.me {
				go func(i int) {
					index := len(rf.Log) - 1
					term := rf.Log[index].Term
					Log.Debug(Log.DInfo, "S%d send heartbeat to [%d],term [%d]applyid[%d]", rf.me, i, rf.TermId,rf.LastApplyId)
					argc := AppendEntriesArgs{Term: rf.TermId, LeaderId: rf.me, LeaderApply: rf.LastApplyId, PrevLogTerm: term, PrevLogIndex: index+rf.LastIncludedIndex}
					reply := AppendEntriesReply{}
					ret := rf.peers[i].Call("Raft.AppendEntries", &argc, &reply)
					if !ret {
						//Log.Debug(Log.DLog, "S%d send heartbeat to [%d] no return", rf.me, i)
						return
					}
					if reply.Term > rf.TermId {
						rf.TermId = reply.Term
						rf.Role = Follower
						rf.LastVoted = -1
						rf.persist()
					}

				}(i)
			}
		}
		time.Sleep(time.Duration(HEARTBEATPERIOD) * time.Millisecond)
	}
}
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.TermId++
	rf.LastVoted = rf.me
	rf.Role = Candidates
	rf.TimeStamp = time.Now()
	term := rf.TermId
	rf.persist()
	rf.mu.Unlock()

	VoteCount := 1
	for i := range rf.peers {
		if rf.me == i {
			Log.Debug(Log.DVote, "S%d self vote term[%d] ", rf.me, rf.TermId)
			continue
		}
		go func(i int) {
			LastLogIdx := len(rf.Log) - 1
			LastLogTermId := rf.Log[LastLogIdx].Term
			argc := RequestVoteArgs{VotedId: rf.me, TermId: term, LogLen: len(rf.Log)+rf.LastIncludedIndex, LastLogTermId: LastLogTermId}
			reply := RequestVoteReply{}
			if rf.TermId != term {
				return
			}
			ok := rf.sendRequestVote(i, &argc, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Selected && rf.Role == Candidates {
					VoteCount++
					if VoteCount*2 > len(rf.peers) {
						Log.Debug(Log.DLeader, "S%d elected, term [%d]", rf.me, rf.TermId)
						rf.Role = Leader
						rf.TimeStamp = time.Now()
						// set NextIndex[]
						for i := range rf.NextIndex {
							rf.NextIndex[i] = len(rf.Log) + rf.LastIncludedIndex
						}
						go rf.SendHeartBeat()
						go rf.AppendTicker()

					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
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

	// Your initialization code here (2A, 2B, 2C).

	rf.ApplyCh = applyCh
	rf.Role = Follower
	rf.LastVoted = -1
	rf.TermId = 0
	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{Term: 0})
	Log.Debug(Log.DTest, "S%d start now", rf.me)
	//rf.Log = append(rf.Log, LogEntry{})
	rf.TimeStamp = time.Now()
	rf.NextIndex = make([]int, len(peers))
	for i := range rf.NextIndex {
		rf.NextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.LastCommitId = rf.LastIncludedIndex
	rf.LastApplyId = rf.LastIncludedIndex
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitTicker()

	return rf
}
