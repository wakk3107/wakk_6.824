package kvraft

import (
	"time"
)

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			DPrintf("S%d apply msg: %+v", kv.me, msg)
			//是快照
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.setSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {
				kv.mu.Lock()
				//不执行过期消息
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf("S%d out time apply(%d <= %d): %+v", kv.me, msg.CommandIndex, kv.lastApplied, msg)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var resp OpResp
				cmd := msg.Command.(Op)
				//重复直接返回缓存
				if cmd.OpType != OpGet && kv.isDuplicate(cmd.ClientId, cmd.SeqId) {
					context := kv.LastCmdContext[cmd.ClientId]
					resp = context.Reply
				} else {
					//不重复则执行命令
					resp.Value, resp.Err = kv.Opt(cmd)
					kv.LastCmdContext[cmd.ClientId] = OpContext{
						SeqId: cmd.SeqId,
						Reply: resp,
					}
				}
				//不是 leader 的话不能给客户端应答
				term, isLeader := kv.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					kv.mu.Unlock()
					continue
				}
				//返回应答
				it := IndexAndTerm{msg.CommandIndex, term}
				ch, ok := kv.cmdRespChans[it]
				if ok {
					//等管道 10 毫秒，能塞最好 不行让客户端下次再试，毕竟已经缓存了回答
					select {
					case ch <- resp:
					case <-time.After(10 * time.Millisecond):
					}
				}

				kv.mu.Unlock()
			} else {
				// 无效命令
			}
		default:
			//不许等！可能会导致RaftStateSize过大通不过测试 ，主要就是 lastApplied 不能及时变化，导致不能进行快照，压缩日志
			//time.Sleep(gap_time)
		}
	}
}

func (kv *KVServer) isDuplicate(clientId int64, seqId int64) bool {
	context, ok := kv.LastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.SeqId {
		return true
	}
	return false
}
