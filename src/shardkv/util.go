package shardkv

import (
	"../labgob"
	"../shardmaster"
	"bytes"
	"log"
)

func (kv *ShardKV) snapshot(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.ownShards)
	e.Encode(kv.migratingShards)
	e.Encode(kv.waitingShards)
	e.Encode(kv.cleaningShards)
	e.Encode(kv.historyConfigs)
	e.Encode(kv.config)
	e.Encode(kv.cache)
	e.Encode(kv.data)

	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(lastCommandIndex, snapshot)
}

func (kv *ShardKV) snapshotIfNeeded(lastCommandIndex int) {
	var threshold = int(SnapshotThreshold * float64(kv.maxraftstate))
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= threshold {
		kv.snapshot(lastCommandIndex)
	}
}

func (kv *ShardKV) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var config shardmaster.Config
	ownShards, migratingShards, waitingShards, cleaningShards := make(IntSet), make(map[int]map[int]MigrationData), make(map[int]int), make(map[int]IntSet)
	historyConfigs, cache, data := make([]shardmaster.Config, 0), make(map[int64]string), make(map[string]string)

	if d.Decode(&ownShards) != nil ||
		d.Decode(&migratingShards) != nil ||
		d.Decode(&waitingShards) != nil ||
		d.Decode(&cleaningShards) != nil ||
		d.Decode(&historyConfigs) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&cache) != nil ||
		d.Decode(&data) != nil {
		log.Fatal("Error in reading snapshot")
	}
	kv.config = config
	kv.ownShards, kv.migratingShards, kv.waitingShards, kv.cleaningShards = ownShards, migratingShards, waitingShards, cleaningShards
	kv.historyConfigs, kv.cache, kv.data = historyConfigs, cache, data
}
