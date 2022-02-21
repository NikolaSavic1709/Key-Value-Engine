package writePath

import (
	"fmt"
	"napredni/structures/CMS"
	"napredni/structures/HLL"
	"napredni/structures/LRU"
	"napredni/structures/LSM"
	"napredni/structures/Memtable"
	"napredni/structures/SStable"
	"napredni/structures/WAL"
	"napredni/structures/readPath"
	"napredni/structures/record"
	"napredni/structures/skipList"
	"napredni/structures/tokenBucket"
	"time"
)

// Put
// tombstone == 0 -> add
// tombstone == 1 -> delete

func InitializeWAL(lwm int, maxNumberOfRecords int) *WAL.WAL {
	wal, err := WAL.CreateWAL("data/wal/", uint8(lwm), uint8(maxNumberOfRecords))
	if err != nil {
		panic(err)
	}
	return wal
}

func InitializeMemTable(capacity float64, threshold float64) *Memtable.MemTable {
	mt := Memtable.MemTable{
		Capacity:  capacity,
		Threshold: threshold,
		Sl:        &skipList.SkipList{},
	}

	return &mt
}

func InitializeLSM(mem *Memtable.MemTable, lsmLevels, LsmLevelMax int) *LSM.LSM {
	return LSM.CreateLSM(*mem, uint8(lsmLevels), uint8(LsmLevelMax))
}

func InitializeTokenBucket(interval time.Duration, maxRequests int) *tokenBucket.TokenBucket {
	return &tokenBucket.TokenBucket{
		Interval:          interval,
		MaxRequests:       maxRequests,
		Start:             time.Now(),
		AvailableRequests: maxRequests,
	}
}

func Put(wal *WAL.WAL,  lsm *LSM.LSM, key string, value []byte) {
	newRecord := record.CreateRecord(key, value, 0)

	if !wal.AddData(key, value) {
		panic(key)
		return
	}
	var records, success, _ = lsm.MemTable.AddRecord(*newRecord)
	if !success {
		return
	}
	if records != nil {
		fmt.Println("Formira se sstable...")

		level := 1
		index := SStable.GetNewIndexForLevel(level)

		filePaths := SStable.FormFilePathsForSSTable(level, index)

		sstable := SStable.FormSSTable(records, filePaths[0], filePaths[1],
			filePaths[2], filePaths[3], filePaths[4], filePaths[5])
		lsm.AddSSTable(*sstable)
		wal.DeleteAllSegments()
		wal.AddData(key, value)
	}
}

func PutHLL(wal *WAL.WAL,  lsm *LSM.LSM, key string){
	hll := HLL.CreateHLL(4)
	value := hll.DecodeHLL()
	Put(wal, lsm, key, value)
}

func PutCMS(wal *WAL.WAL,  lsm *LSM.LSM, key string){
	cms := CMS.CreateCountMinSketch(0.01, 0.01)
	value := cms.DecodeCMS()
	Put(wal, lsm, key, value)
}

func Delete(wal *WAL.WAL, cache *LRU.CacheLRU, lsm *LSM.LSM, key string) bool {
	cache.Remove(key)
	found, _ := readPath.Get(cache, lsm, key)
	if !found {
		return false
	}

	if !wal.DeleteData(key, []byte("0")) {
		panic("Couldn't delete from wal!")
	}

	newRecord := record.CreateRecord(key, []byte("0"), 1)
	var records, _, _ = lsm.MemTable.AddRecord(*newRecord)
	if records != nil {
		fmt.Println("Formira se sstable...")

		level := 1
		index := SStable.GetNewIndexForLevel(level)

		filePaths := SStable.FormFilePathsForSSTable(level, index)

		sstable := SStable.FormSSTable(records, filePaths[0], filePaths[1],
			filePaths[2], filePaths[3], filePaths[4], filePaths[5])
		lsm.AddSSTable(*sstable)
		wal.DeleteAllSegments()
		wal.DeleteData(key, []byte("0"))
	}
	return true
}
