package kvraft

import (
	"6.5840/Log"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key string
	Value string
	SequenceId int
	ClientId int64
}
func (kv * KVServer)MemoryKVGet(key string) string {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	key,ret:=kv.KV[key]
	if ret {
		return key
	}else{
		Log.Debug(Log.DServer,"S%d key:%s not exists",kv.me, key)
		return ""
	}
}
func (kv * KVServer)MemoryKVPutAppend(key string,value string, op string) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if op==APPEND {
		ori,ok :=kv.KV[key]
		if ok{
			kv.KV[key] = ori+value
		}else {
			kv.KV[key] = value
		}
	}else if op==PUT {
		kv.KV[key] = value
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KV map[string]string
	MsgChanMap map[int]chan raft.ApplyMsg
	ClientSeqMap map[int64]int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{GET,args.Key,"",args.Sequence,args.ClientID}
	index,_,leader:=kv.rf.Start(op)
	reply.Err=""
	reply.LeaderId = -1
	if !leader{
		reply.Err= ErrWrongLeader
		reply.Value= ""
		reply.LeaderId = kv.rf.LastVoted
	}else{
		ch := kv.GetIndexCh(index)
		select {
			case <-ch:
				if _, Leader:=kv.rf.GetState(); Leader {
					kv.mu.Lock()
					value:=kv.MemoryKVGet(args.Key)
					kv.mu.Unlock()
					reply.Value = value
					reply.Err = OK
				}else{
					reply.Err= ErrWrongLeader
					reply.LeaderId = kv.rf.LastVoted
				}
				break
			case <-time.After(time.Duration(raft.TIMEOUTPERIOD)*time.Millisecond):
				reply.Err = ErrTimeout
		}

	}

}
func (kv * KVServer) DuplicateRequest(ClientID int64,Sequence int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Seq,ok:=kv.ClientSeqMap[ClientID]
	if !ok{
		//kv.ClientSeqMap[ClientID]=Sequence -1
		//Seq = kv.ClientSeqMap[ClientID]
		return false
	}else{
		if Sequence<=Seq{
			//reply.Err = ErrOldRequest
			//reply.LeaderId = -1
			return true
		}
	}
	return false
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.DuplicateRequest(args.ClientID,args.Sequence){
		reply.Err = ErrOldRequest
		reply.LeaderId = -1
		return
	}

	op := Op{args.Op,args.Key,args.Value,args.Sequence,args.ClientID}
	index,_,leader:=kv.rf.Start(op)
	reply.LeaderId = -1
	reply.Err=OK
	if !leader{
		reply.Err= ErrWrongLeader
		reply.LeaderId = kv.rf.LastVoted
	}else{
		ch := kv.GetIndexCh(index)
		select {
		case <-ch:
			if _, Leader:=kv.rf.GetState(); Leader {
				//kv.mu.Lock()
				//kv.MemoryKVPutAppend(args.Key,args.Value,args.Op)
				//kv.mu.Lock()
				//kv.ClientSeqMap[args.ClientID] = args.Sequence
				//kv.mu.Unlock()
			}
			break
		case <-time.After(time.Duration(raft.TIMEOUTPERIOD)*time.Millisecond):
			reply.Err = ErrTimeout
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv * KVServer) ProcessMsg() {
	for {
		//time.Sleep(50*time.Millisecond)
		if kv.killed(){
			return
		}
		//Log.Debug(Log.DLog,"S%d Process before", kv.me)
		msg:=<-kv.rf.ApplyCh

		//Log.Debug(Log.DLog,"S%d Process after", kv.me)
		op := msg.Command.(Op)
		if op.Command==APPEND || op.Command==PUT{
			if !kv.DuplicateRequest(op.ClientId,op.SequenceId){
				Log.Debug(Log.DServer,"S%d Op:%s key:%s,v:%s,Clt:%v,Seq:%v",kv.me,op.Command,op.Key,op.Value,op.ClientId,op.SequenceId)
				kv.mu.Lock()
				kv.MemoryKVPutAppend(op.Key,op.Value,op.Command)
				//kv.mu.Lock()
				kv.ClientSeqMap[op.ClientId] = op.SequenceId
				Log.Debug(Log.DServer,"S%d after op:%s,v:%s，client:%v,seq:%v",kv.me,op.Command,kv.MemoryKVGet(op.Key),op.ClientId,op.SequenceId)
				kv.mu.Unlock()
			}
			//break
		}else{
			kv.mu.Lock()
			kv.ClientSeqMap[op.ClientId] = op.SequenceId
			kv.mu.Unlock()
		}

		ch := kv.GetIndexCh(msg.CommandIndex)
		ch <- msg
	}
}
func (kv * KVServer)GetIndexCh(index int) chan raft.ApplyMsg{
	ch,ret:= kv.MsgChanMap[index]
	if ret{
		return ch
	}else{
		kv.MsgChanMap[index] = make(chan raft.ApplyMsg,1)
		return kv.MsgChanMap[index]
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.KV = make(map[string]string)
	kv.MsgChanMap = make(map[int]chan raft.ApplyMsg)
	kv.ClientSeqMap = make(map[int64]int)
	go kv.ProcessMsg()
	// You may need initialization code here.

	return kv
}
