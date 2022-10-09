package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_TYPE_PUT    = "Put"
	OP_TYPE_APPEND = "Append"
	OP_TYPE_GET    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index    int    // 写入raft log时的index
	Term     int    // 写入raft log时的term
	Type     string // PutAppend, Get
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *Op
	committed chan byte

	wrongLeader bool // 因为index位置log的term不一致, 说明leader换过了
	ignored     bool // 因为req id过期, 导致该日志被跳过

	// Get操作的结果
	keyExist bool
	value    string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string  // kv存储
	reqMap  map[int]*OpContext // log index -> 请求上下文
	seqMap  map[int64]int64    // 客户端id -> 客户端seq
}

func newOpContext(op *Op) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK

	op := &Op{
		Type:     OP_TYPE_GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kv.reqMap[op.Index] = opCtx
	}()

	// RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist { // key不存在
			reply.Err = ErrNoKey
		} else {
			reply.Value = opCtx.value // 返回值
			//fmt.Println("完成get", time.Now())
		}
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK

	op := &Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kv.reqMap[op.Index] = opCtx
	}()

	// RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if opCtx.ignored {
			// 说明req id过期了，该请求被忽略，对MIT这个lab来说只需要告知客户端OK跳过即可
		}
		//fmt.Println("完成put/append", time.Now())
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		time.Sleep(time.Microsecond * 10)
		select {
		case msg := <-kv.applyCh:
			cmd := msg.Command
			index := msg.CommandIndex

			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				var op Op
				switch r := cmd.(type) {
				case *Op:
					op = *r
				case Op:
					op = r
				default:
					return
				}

				opCtx, existOp := kv.reqMap[index]
				prevSeq, existSeq := kv.seqMap[op.ClientId]
				kv.seqMap[op.ClientId] = op.SeqId

				if existOp { // 存在等待结果的RPC, 那么判断状态是否与写入时一致
					if opCtx.op.Term != op.Term {
						opCtx.wrongLeader = true
					}
				}

				// 只处理ID单调递增的客户端写请求
				if op.Type == OP_TYPE_PUT || op.Type == OP_TYPE_APPEND {
					if !existSeq || op.SeqId > prevSeq { // 如果是递增的请求ID，那么接受它的变更
						if op.Type == OP_TYPE_PUT { // put操作
							kv.kvStore[op.Key] = op.Value
						} else if op.Type == OP_TYPE_APPEND { // put-append操作
							if val, exist := kv.kvStore[op.Key]; exist {
								kv.kvStore[op.Key] = val + op.Value
							} else {
								kv.kvStore[op.Key] = op.Value
							}
						}
					} else if existOp {
						opCtx.ignored = true
					}
				} else { // OP_TYPE_GET
					if existOp {
						opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
					}
				}
				DPrintf("RaftNode[%d] applyLoop, kvStore[%v]", kv.me, kv.kvStore)

				// 唤醒挂起的RPC
				if existOp {
					close(opCtx.committed)
				}
			}()
		}
	}
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
	kv.rf.Kill()
	// Your code here, if desired.
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)

	go kv.applyLoop()
	return kv
}
