package kvraft

import (
	"bytes"
	"log"
	"time"

	"6.824/labgob"
)

const threshold float32 = 0.8
const snapshotLogGap int = 3

func (kv *KVServer) snapshoter() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot+snapshotLogGap {
			kv.doSnapshot(kv.lastApplied)
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

func (kv *KVServer) isNeedSnapshot() bool {
	//如果maxraftstate为-1，则快照功能关闭。
	if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > kv.maxraftstate {
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

func (kv *KVServer) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
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
}
