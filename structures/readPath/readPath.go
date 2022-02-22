package readPath

import (
	"napredni/structures/LRU"
	"napredni/structures/LSM"
	"napredni/structures/WAL"
	"napredni/structures/bloomFilter"
	"napredni/structures/record"
)

func Get(cache *LRU.CacheLRU, lsm *LSM.LSM, key string) (bool, []byte){
	found, value:=lsm.MemTable.GetRecord(key)
	if found {
		cache.Add(key, value)
		return true, value
	}else{
		if value!=nil{
			return false,nil
		}
	}
	found, value = cache.Get(key)
	if found{
		return true, value
	}

	mostRecentRecord := record.Record{}
	var foundInSSTable bool
	for i:=0;i<int(lsm.MaxNumOfLvl);i++{
		for j:=0;j<len(lsm.Levels[i]);j++{
			bfPath := lsm.Levels[i][j].FilterFilePath
			bFilter := bloomFilter.BloomFilter{} //parametri
			// bFilter := bloomFilter.createBloomFilter(100, 0.01)
			bFilter.DecodeBloomFilter(bfPath)
			found = bFilter.FindData(key)
			if !found {
				continue
			}

			tmpRecord, found := lsm.Levels[i][j].GetRecordInSStableForKey(key)
			if !found {
				continue
			}

			if tmpRecord.Timestamp > mostRecentRecord.Timestamp {
				foundInSSTable = true
				mostRecentRecord = *tmpRecord
			}
		}
	}
	if found || foundInSSTable{
		if mostRecentRecord.Tombstone ==1 {
			return false, nil

		} else {
			if WAL.CRC32(mostRecentRecord.Value) != mostRecentRecord.Crc {
				panic("CRC is not compatible!")
				return false, nil //?
			}
			cache.Add(mostRecentRecord.Key, mostRecentRecord.Value)
			return true, mostRecentRecord.Value
		}
	}

	return false, nil
}
