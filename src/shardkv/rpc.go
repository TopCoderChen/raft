package shardkv

// RPC Handlers.

// Another server has requested the shard specified in the args.
// Put the shard to be migrated in the reply (deep copied).
func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err, reply.Shard, reply.ConfigNum = OK, args.Shard, args.ConfigNum
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	reply.MigrationData = MigrationData{Data: make(map[string]string), Cache: make(map[int64]string)}
	// migratingShards: config number -> shard -> migration data
	if v, ok := kv.migratingShards[args.ConfigNum]; ok {
		if migrationData, ok := v[args.Shard]; ok {
			for k, v := range migrationData.Data {
				reply.MigrationData.Data[k] = v
			}
			for k, v := range migrationData.Cache {
				reply.MigrationData.Cache[k] = v
			}
		}
	}
}

// When the other group finishes shard migration,
// it uses this RPC handler to clean up useless shard in source (this local) group.
func (kv *ShardKV) ShardCleanup(args *ShardCleanupArgs, reply *ShardCleanupReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.migratingShards[args.ConfigNum]; ok {
		if _, ok := kv.migratingShards[args.ConfigNum][args.Shard]; ok {
			// avoid deadlock
			kv.mu.Unlock()
			result, _ := kv.start(kv.getConfigNum(), args.copy())
			reply.Err = result
			kv.mu.Lock()
		}
	}
}
