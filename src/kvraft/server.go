package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // maxraftstate表示允许的持久Raft状态的最大字节大小

	// Your definitions here.
	KvMap          *KV
	cmdRespChans   map[IndexAndTerm]chan OpResp
	LastCmdContext map[int64]OpContext
	lastApplied    int
	lastSnapshot   int
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("---kill\n")
	kv.doSnapshot(kv.lastApplied)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KvMap = NewKV()
	kv.cmdRespChans = make(map[IndexAndTerm]chan OpResp)
	kv.LastCmdContext = make(map[int64]OpContext)
	kv.lastApplied = 0
	kv.lastSnapshot = 0

	// load data from persister
	kv.setSnapshot(persister.ReadSnapshot())

	// long-time goroutines
	go kv.applier()
	go kv.snapshoter()

	return kv
}

func (kv *KVServer) Command(args *CmdArgs, reply *CmdReply) {
	kv.mu.Lock()
	//去重，如果非幂等请求重复了，则返回上一次的回答
	if args.OpType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
		context := kv.LastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.Reply.Value, context.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		OpType:   args.OpType,
		Key:      args.Key,
		Value:    args.Value,
	}
	//若不是 leader 的话该 cmd 不会传给 raft 层的，start的内部实现
	index, term, is_leader := kv.rf.Start(cmd)
	if !is_leader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}
	kv.mu.Lock()
	it := IndexAndTerm{index, term}
	//创建 Wait Channel 等待回答
	ch := make(chan OpResp, 1)
	//根据索引放起来，以便 Server 的 ApplyLoop 执行完后，能够将 Reply 正确的塞入 Wait Channel
	kv.cmdRespChans[it] = ch
	kv.mu.Unlock()

	defer func() {
		//结束完记得删除 Wait Channel 清理空间
		kv.mu.Lock()
		delete(kv.cmdRespChans, it)
		kv.mu.Unlock()
		close(ch)
	}()
	//若超时了，就告诉客户端该情况
	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		//返回结果
		case resp := <-ch:
			reply.Value, reply.Err = resp.Value, resp.Err
			kv.mu.Unlock()
			return
			//超时，让客户端一会再试
		case <-t.C:
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			// 不知道为啥，就得 sleep，不然过不了 unreliable net, restarts, partitions, random keys, many clients
			time.Sleep(gap_time)
		}
	}
}
