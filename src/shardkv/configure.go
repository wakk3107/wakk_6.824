package shardkv

import (
	"6.824/shardctrler"
)

func (kv *ShardKV) configureAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	//若有一个分片正在迁移，则不能采取新配置
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			Debug(dWarn, "G%+v S%d shard: %+v", kv.gid, kv.me, shard)
			canPerformNextConfig = false
			break
		}
	}
	//执行大于当前配置版本一代的配置
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &OpResp{})
		}
	} else {
		Debug(dWarn, "G%+v {S%+v} don't need fetch config!", kv.gid, kv.me)
	}
}

//更新配置
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OpResp {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig.DeepCopy()
		kv.currentConfig = nextConfig.DeepCopy()
		Debug(dWarn, "G%+v {S%+v} applyConfiguration %d is %+v", kv.gid, kv.me, nextConfig.Num, nextConfig)
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrTimeoutReq, ""}
}

//更新分片状态
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	// special judge
	if nextConfig.Num == 1 {
		shards := kv.getAllShards(nextConfig)
		for _, shard := range shards {
			kv.shards[shard] = NewShard(Serving)
		}
		return
	}

	newShards := kv.getAllShards(nextConfig)
	nowShards := kv.getAllShards(&kv.currentConfig)
	// loss shard
	for _, nowShard := range nowShards {
		if nextConfig.Shards[nowShard] != kv.gid {
			// BePulling
			kv.shards[nowShard].Status = BePulling
		}
	}
	// get shard
	for _, newShard := range newShards {
		if kv.currentConfig.Shards[newShard] != kv.gid {
			// Pulling
			kv.shards[newShard] = NewShard(Pulling)
		}
	}
}

func (kv *ShardKV) getAllShards(nextConfig *shardctrler.Config) []int {
	var shards []int
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid {
			shards = append(shards, shard)
		}
	}
	return shards
}

//根据状态获取分片
func (kv *ShardKV) getShardIDsByStatus(status ShardStatus, config *shardctrler.Config) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shard, _ := range kv.shards {
		if kv.shards[shard].Status == status {
			gid := config.Shards[shard]
			if _, ok := gid2shardIDs[gid]; !ok {
				vec := [1]int{shard}
				gid2shardIDs[gid] = vec[:]
			} else {
				gid2shardIDs[gid] = append(gid2shardIDs[gid], shard)
			}
		}
	}
	return gid2shardIDs
}
