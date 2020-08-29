package shardmaster

// Shardmaster applies the Join request.
// Distribute the shards as evenly as possible.
func (sm *ShardMaster) applyJoin(arg JoinArgs) {
	if sm.cache[arg.ClientId] >= arg.RequestSeq { // duplicate req.
		return
	}
	newConfig := sm.getConfig(-1) // create new configuration based on last configuration
	newGIDS := make([]int, 0)
	for gid, server := range arg.Servers {
		if s, ok := newConfig.Groups[gid]; ok {
			newConfig.Groups[gid] = append(s, server...)
		} else {
			newConfig.Groups[gid] = append([]string{}, server...)
			newGIDS = append(newGIDS, gid)
		}
	}
	if len(newConfig.Groups) == 0 { // no replica group exists
		newConfig.Shards = [NShards]int{}
	} else if len(newConfig.Groups) <= NShards {
		shardsByGID := make(map[int]int) // gid -> number of shards it owns
		var minShardsPerGID, maxShardsPerGID, maxShardsPerGIDCount int
		minShardsPerGID = NShards / len(newConfig.Groups)
		if maxShardsPerGIDCount = NShards % len(newConfig.Groups); maxShardsPerGIDCount != 0 {
			maxShardsPerGID = minShardsPerGID + 1
		} else {
			maxShardsPerGID = minShardsPerGID
		}
		// divide shards as evenly as possible among replica groups and move as few shards as possible
		for i, j := 0, 0; i < NShards; i++ {
			gid := newConfig.Shards[i]
			if gid == 0 ||
				(minShardsPerGID == maxShardsPerGID && shardsByGID[gid] == minShardsPerGID) ||
				(minShardsPerGID < maxShardsPerGID && shardsByGID[gid] == minShardsPerGID && maxShardsPerGIDCount <= 0) {
				newGID := newGIDS[j]
				newConfig.Shards[i] = newGID
				shardsByGID[newGID] += 1
				j = (j + 1) % len(newGIDS)
			} else {
				shardsByGID[gid] += 1
				if shardsByGID[gid] == minShardsPerGID {
					maxShardsPerGIDCount -= 1
				}
			}
		}
	}
	DPrintf("[Join] ShardMaster %d append new Join config, newGIDS: %v, config: %v", sm.me, newGIDS, newConfig)
	sm.cache[arg.ClientId] = arg.RequestSeq
	sm.appendNewConfig(newConfig)
}

func (sm *ShardMaster) applyLeave(arg LeaveArgs) {
	if sm.cache[arg.ClientId] >= arg.RequestSeq { // duplicate req.
		return
	}

	newConfig := sm.getConfig(-1) // create new configuration

	// GIDs that left the whole service.
	leaveGIDs := make(map[int]struct{})

	for _, gid := range arg.GIDs {
		delete(newConfig.Groups, gid)
		leaveGIDs[gid] = struct{}{}
	}
	if len(newConfig.Groups) == 0 { // remove all gid
		newConfig.Shards = [NShards]int{}
	} else {
		// New config, divide the shards as evenly as possible among the groups
		remainingGIDs := make([]int, 0)
		for gid := range newConfig.Groups {
			remainingGIDs = append(remainingGIDs, gid)
		}
		shardsPerGID := NShards / len(newConfig.Groups) // NShards / total number of gid
		if shardsPerGID < 1 {
			shardsPerGID = 1
		}
		shardsByGID := make(map[int]int)
	loop:
		for i, j := 0, 0; i < NShards; i++ {
			gid := newConfig.Shards[i]
			if _, ok := leaveGIDs[gid]; ok || shardsByGID[gid] == shardsPerGID {
				for _, id := range remainingGIDs {
					if shardsByGID[id] < shardsPerGID {
						newConfig.Shards[i] = id
						shardsByGID[id] += 1
						continue loop
					}
				}
				// Assign the remaining GIDs linearly to the shards left
				id := remainingGIDs[j]
				j = (j + 1) % len(remainingGIDs)
				newConfig.Shards[i] = id
				shardsByGID[id] += 1
			} else {
				shardsByGID[gid] += 1
			}
		}

		DPrintf("[Leave] ShardMaster %d append new Leave config, shardsPerGID: %d, remainingGIDS: %v, config: %v", sm.me, shardsPerGID, remainingGIDs, newConfig)
	}

	sm.cache[arg.ClientId] = arg.RequestSeq
	sm.appendNewConfig(newConfig)
}

func (sm *ShardMaster) applyMove(arg MoveArgs) {
	if sm.cache[arg.ClientId] >= arg.RequestSeq { // duplicate req.
		return
	}
	newConfig := sm.getConfig(-1)
	newConfig.Shards[arg.Shard] = arg.GID
	DPrintf("[Move] ShardMaster %d append new Move config: %#v", sm.me, newConfig)
	sm.cache[arg.ClientId] = arg.RequestSeq
	sm.appendNewConfig(newConfig)
}
