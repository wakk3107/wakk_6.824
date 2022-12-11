package kvraft

import (
	"bytes"
	"log"
	"time"

	"6.824/labgob"
)

func (kv *KVServer) snapshoter() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot {
			//fmt.Println("压缩日志前，SnapShotSize：", kv.rf.RaftPersistSize())
			kv.doSnapshot(kv.lastApplied)
			//fmt.Println("压缩日志后，SnapShotSize：", kv.rf.RaftPersistSize())

			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

func (kv *KVServer) isNeedSnapshot() bool {
	//如果maxraftstate为-1，则快照功能关闭。
	if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > kv.maxraftstate {
		// if kv.rf.RaftPersistSize() > 5000 {
		// 	fmt.Printf("S%v need SnapShot , now RaftStateSize is:%v\n", kv.me, kv.rf.RaftPersistSize())
		// }
		return true
	}
	return false
}

func (kv *KVServer) doSnapshot(commandIndex int) {
	DPrintf("S%d doSnapshot", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(*kv.KvMap) != nil ||
		e.Encode(kv.LastCmdContext) != nil {
		panic("server doSnapshot encode error")
	}
	kv.rf.Snapshot(commandIndex, w.Bytes())
}

// 应用 应用层 快照
func (kv *KVServer) setSnapshot(snapshot []byte) bool {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return false
	}

	DPrintf("S%d setSnapshot", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap KV
	var lastCmdContext map[int64]OpContext

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastCmdContext) != nil {
		log.Fatalf("server setSnapshot decode error\n")
	} else {
		kv.KvMap = &kvMap
		kv.LastCmdContext = lastCmdContext
	}
	return true
}
