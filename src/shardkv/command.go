package shardkv

import (
	"fmt"

	"6.824/raft"
	"6.824/shardctrler"
)

type Command struct {
	Op   CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(args *CmdArgs) Command {
	return Command{Operation, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(pullReply *PullDataReply) Command {
	return Command{InsertShards, *pullReply}
}

func NewDeleteShardsCommand(pullArgs *PullDataArgs) Command {
	return Command{DeleteShards, *pullArgs}
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
)

// Handler
func (kv *ShardKV) Command(args *CmdArgs, reply *CmdReply) {
	defer Debug(dTrace, "G%+v {S%+v} args: %+v reply: %+v", kv.gid, kv.me, args, reply)

	kv.mu.Lock()
	shardID := key2shard(args.Key)
	if !kv.canServe(shardID) {
		Debug(dWarn, "G%+v {S%+v} shard %d is %+v, can't servering(%+v)", kv.gid, kv.me, shardID, kv.shards[shardID], kv.currentConfig.Shards[shardID])
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.OpType != OpGet && kv.isDuplicate(shardID, args.ClientId, args.SeqId) {
		context := kv.shards[shardID].LastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.Reply.Value, context.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var resp OpResp
	kv.Execute(NewOperationCommand(args), &resp)
	reply.Value, reply.Err = resp.Value, resp.Err
}

// 当传入的切片 Id 属于自己管辖，且该切片的状态属于可提供服务状态
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.shards[shardID].Status == Serving || kv.shards[shardID].Status == GCing)
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, cmd *CmdArgs) *OpResp {
	shardID := key2shard(cmd.Key)
	if kv.canServe(shardID) {
		if cmd.OpType != OpGet && kv.isDuplicate(shardID, cmd.ClientId, cmd.SeqId) {
			context := kv.shards[shardID].LastCmdContext[cmd.ClientId]
			return &context.Reply
		} else {
			var resp OpResp
			resp.Value, resp.Err = kv.Opt(cmd, shardID)
			kv.shards[shardID].LastCmdContext[cmd.ClientId] = OpContext{
				SeqId: cmd.SeqId,
				Reply: resp,
			}
			return &resp
		}
	}
	return &OpResp{ErrWrongGroup, ""}
}
