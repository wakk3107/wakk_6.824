package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	clientId int64
	seqId    int64
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) sendCmd(args CommandArgs) CommandReply {
	ck.seqId += 1
	args.SeqId = ck.seqId
	args.ClientId = ck.clientId

	for {
		reply := CommandReply{}
		//这里的 ok 指的是 Rpc是否调用成功
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Command", &args, &reply)
		//如果该 Server 无法响应 RPC 则换一个
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(retry_timeout)
			continue
		}

		if reply.Err == OK {
			return reply
		}
		//如果该 Server 不是 Leader 则换下一个
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(retry_timeout)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := CommandArgs{
		Op:  OpQuery,
		Num: num,
	}
	reply := ck.sendCmd(args)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CommandArgs{
		Op:      OpJoin,
		Servers: servers,
	}
	// reply := ck.sendCmd(args)
	ck.sendCmd(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := CommandArgs{
		Op:   OpLeave,
		GIDs: gids,
	}
	ck.sendCmd(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CommandArgs{
		Op:    OpMove,
		Shard: shard,
		GID:   gid,
	}
	ck.sendCmd(args)
}
