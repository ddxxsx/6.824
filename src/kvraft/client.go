package kvraft

import (
	"6.5840/Log"
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int
	ClientId int64
	Sequence int

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.Sequence = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	Log.Debug(Log.DClient,"C%d get k:%s",ck.ClientId,key)
	for {
		args:= GetArgs{key,ck.ClientId,ck.Sequence}
		reply:=GetReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.Get",&args,&reply)
		if !ok{
			Log.Debug(Log.DError,"C%d get return false",ck.ClientId)
			ck.LeaderId =  (ck.LeaderId+1)%len(ck.servers)
			continue
		}
		Log.Debug(Log.DClient,"C%d get reply v:%s,e:%v",ck.ClientId,reply.Value,reply.Err)
		if reply.Err==OK {
			//Log.Debug(Log.DClient,"C%d get ok,v:%s",ck.ClientId,reply.Value)
			ck.Sequence++
			return reply.Value
		}else if reply.Err==ErrWrongLeader{
			if reply.LeaderId == -1{
				ck.LeaderId =  (ck.LeaderId+1)%len(ck.servers)
			}else{
				ck.LeaderId = reply.LeaderId
			}
			continue
		}else if reply.Err== ErrTimeout{
			continue
		}else {
			ck.Sequence++
			return ""
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	for {
		args:= PutAppendArgs{key,value,op,ck.ClientId,ck.Sequence}
		reply:=PutAppendReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend",&args,&reply)
		if !ok{
			Log.Debug(Log.DError,"C%d put append return false",ck.ClientId)
			ck.LeaderId =  (ck.LeaderId+1)%len(ck.servers)
			continue
		}
		if reply.Err==OK||reply.Err == ErrOldRequest {
			ck.Sequence++
			return
		}else if reply.Err==ErrWrongLeader{
			if reply.LeaderId == -1{
				ck.LeaderId =  (ck.LeaderId+1)%len(ck.servers)
			}else{
				ck.LeaderId = reply.LeaderId
			}
			continue
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	Log.Debug(Log.DClient,"C%d put k:%s,v:%s",ck.ClientId,key,value)
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	Log.Debug(Log.DClient,"C%d append k:%s,v:%s",ck.ClientId,key,value)
	ck.PutAppend(key, value, APPEND)
}
