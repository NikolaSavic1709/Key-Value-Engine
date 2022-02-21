package LSM

import (
	"io/ioutil"
	"napredni/structures/Memtable"
	"napredni/structures/SStable"
	"napredni/structures/record"
)

type LSM struct {
	MemTable            Memtable.MemTable
	Levels              [][]SStable.SSTable
	MaxNumOfLvl         uint8
	MaxNumOfTablesInLvl uint8
}

func CreateLSM(memtable Memtable.MemTable, numOfLevels uint8, numOfTablesInLevel uint8) *LSM {
	lsm := &LSM{}
	lsm.MemTable = memtable
	lsm.Levels = make([][]SStable.SSTable, numOfLevels)
	//for i := range lsm.Levels {
	//	lsm.Levels[i] = make([]SStable.SSTable, 0)
	//}
	lsm.MaxNumOfLvl = numOfLevels
	lsm.MaxNumOfTablesInLvl = numOfTablesInLevel
	return lsm
}

func (lsm *LSM) UpdateLSM() {
	files, _ := ioutil.ReadDir("data/data")

	if len(files) == 0 {
		return
	}
	maxIdx := make([]int, lsm.MaxNumOfLvl)
	for _, file := range files {
		fileName := file.Name()

		level, index := SStable.GetLevelAndIndexForFileName(fileName)
		level--
		if index > maxIdx[level] {
			maxIdx[level] = index
		}
	}
	for i := 0; i < len(lsm.Levels); i++ {
		if maxIdx[i] != 0 {
			lsm.Levels[i] = make([]SStable.SSTable, maxIdx[i])
		} else {
			lsm.Levels[i] = make([]SStable.SSTable, 0)
		}
	}

	for _, file := range files {
		fileName := file.Name()

		level, index := SStable.GetLevelAndIndexForFileName(fileName)
		filePaths := SStable.FormFilePathsForSSTable(level, index)
		sstable := SStable.SSTable{DataFilePath: filePaths[0], IndexFilePath: filePaths[1],
			SummaryFilePath: filePaths[2], FilterFilePath: filePaths[3], MetadataFilePath: filePaths[4],
			TOCFilePath: filePaths[5]}
		lsm.Levels[level-1][index-1] = sstable
	}
}

// prima novi sstable koji je nastao flushovanjem memtable
func (lsm *LSM) AddSSTable(sstable SStable.SSTable) {

	lsm.Levels[0] = append(lsm.Levels[0], sstable)
	lvl := 0
	for {
		if uint8(len(lsm.Levels[lvl])) < lsm.MaxNumOfTablesInLvl {
			break
		}
		nextLvlSstable := MergeTables(lsm.Levels[lvl], lvl)
		for i := 0; i < len(lsm.Levels[lvl]); i++ {
			lsm.Levels[lvl][i].DeleteSSTable()
		}
		lsm.Levels[lvl] = make([]SStable.SSTable, 0)
		lvl++
		lsm.Levels[lvl] = append(lsm.Levels[lvl], *nextLvlSstable)

		if lvl == int(lsm.MaxNumOfLvl)-1 {
			break
		}
	}
}

func MergeTables(sstables []SStable.SSTable, lvl int) *SStable.SSTable {
	sstable1 := sstables[0]
	records := sstable1.GetRecordsFromDataFile()
	for i := 1; i < len(sstables); i++ {
		newRecords := sstables[i].GetRecordsFromDataFile()
		records = MergeData(records, newRecords)
	}
	newIndex := SStable.GetNewIndexForLevel(lvl + 2)
	filepaths := SStable.FormFilePathsForSSTable(lvl+2, newIndex)
	newSstable := SStable.FormSSTable(records, filepaths[0], filepaths[1], filepaths[2], filepaths[3], filepaths[4],
		filepaths[5])
	return newSstable
}

func MergeData(records1 []record.Record, records2 []record.Record) []record.Record {

	records := make([]record.Record, 0)

	it1 := 0
	it2 := 0
	for {
		if records1[it1].Key == records2[it2].Key {
			if records1[it1].Timestamp > records2[it2].Timestamp {
				if records1[it1].Tombstone == 0 {
					records = append(records, records1[it2])
				}
			} else {
				if records2[it2].Tombstone == 0 {
					records = append(records, records2[it2])
				}
			}
			it1++
			if it1 == len(records1) {
				ReadUntilEnd(&records2, it2, &records)
				break
			}
			it2++
			if it2 == len(records2) {
				ReadUntilEnd(&records1, it1, &records)
				break
			}
		} else {
			if records1[it1].Key > records2[it2].Key {
				records = append(records, records2[it2])

				it2++
				if it2 == len(records2) {
					ReadUntilEnd(&records1, it1, &records)
					break
				}
			} else{
				records = append(records, records1[it1])

				it1++
				if it1 == len(records1) {
					ReadUntilEnd(&records2, it2, &records)
					break
				}
			}
		}
	}
	return records
}

func ReadUntilEnd(oldRecords *[]record.Record, it int, records *[]record.Record) {

	for {
		if (*oldRecords)[it].Tombstone == 0 {
			*records = append(*records, (*oldRecords)[it])
		}
		it++
		if it == len(*oldRecords) {
			break
		}
	}
}

func sliceContainsDeletedEL(recordSlice []record.Record) bool {
	for _, record := range recordSlice {
		if record.Tombstone == 1 {
			return true
		}
	}
	return false
}

//func MergeData(sstable1 SStable.SSTable, sstable2 SStable.SSTable) []record.Record {
//	data1, err := os.OpenFile(sstable1.DataFilePath, os.O_RDONLY, 0777)
//	if err != nil {
//		panic(err)
//	}
//	defer data1.Close()
//	data2, err := os.OpenFile(sstable2.DataFilePath, os.O_RDONLY, 0777)
//	if err != nil {
//		panic(err)
//	}
//	defer data2.Close()
//
//	reader1 := bufio.NewReader(data1)
//	reader2 := bufio.NewReader(data2)
//	record1 := record.Record{}
//	record2 := record.Record{}
//	records := make([]record.Record, 0)
//
//	record1.DecodeRecord(reader1)
//	record2.DecodeRecord(reader2)
//	var eof bool
//	for {
//		if record1.Key == record2.Key {
//			if record1.Timestamp > record2.Timestamp {
//				if record1.Tombstone == 0 {
//					records = append(records, record1)
//				}
//			} else {
//				if record2.Tombstone == 0 {
//					records = append(records, record2)
//				}
//			}
//			eof = record1.DecodeRecord(reader1)
//			if eof {
//				ReadUntilEOF(reader2, records)
//				break
//			}
//			eof = record2.DecodeRecord(reader2)
//			if eof {
//				ReadUntilEOF(reader1, records)
//				break
//			}
//		} else {
//			if record1.Key > record2.Key {
//				if record2.Tombstone == 0 {
//					records = append(records, record2)
//				}
//				eof = record2.DecodeRecord(reader2)
//				if eof == true {
//					ReadUntilEOF(reader1, records)
//					break
//				}
//			} else {
//				if record1.Tombstone == 0 {
//					records = append(records, record1)
//				}
//				eof = record1.DecodeRecord(reader1)
//				if eof == true {
//					ReadUntilEOF(reader2, records)
//					break
//				}
//			}
//		}
//	}
//	return records
//}

//za pravljenje novog nivoa prosledjuju se imena k fajlova koji se merguju
//za dodavanje novog na prvi nivo prosledjuje se ime poslednjeg na prvom nivou

//func GetNameOfFiles(fileNames []string) []string {
//	//usertable_level_index_t
//	if len(fileNames) == 1 {
//		ar := strings.Split(fileNames[0], "_")
//		intk, err := strconv.Atoi(ar[2])
//		if err != nil {
//			panic(err)
//		}
//		intk++
//		res := []string{"usertable_1_" + strconv.Itoa(intk) + "_data.db",
//			"usertable_1_" + strconv.Itoa(intk) + "_index.db",
//			"usertable_1_" + strconv.Itoa(intk) + "_summary.db",
//			"usertable_1_" + strconv.Itoa(intk) + "_bfilter.db",
//			"usertable_1_" + strconv.Itoa(intk) + "_metadata.db"}
//
//		return res
//
//	} else {
//		k := len(fileNames)
//		ar := strings.Split(fileNames[len(fileNames) - 1], "_")
//		intk, err := strconv.Atoi(ar[2])
//		if err != nil {
//			panic(err)
//		}
//		newLevel, err := strconv.Atoi(ar[1])
//		if err != nil {
//			panic(err)
//		}
//		intk = intk/k + 1
//		newLevel++
//		res := []string{"usertable_" + strconv.Itoa(newLevel) + "_" + strconv.Itoa(intk) + "_data.db",
//			"usertable_" + strconv.Itoa(newLevel) + "_" + strconv.Itoa(intk) + "_index.db",
//			"usertable_" + strconv.Itoa(newLevel) + "_" + strconv.Itoa(intk) + "_summary.db",
//			"usertable_" + strconv.Itoa(newLevel) + "_" + strconv.Itoa(intk) + "_bfilter.db",
//			"usertable_" + strconv.Itoa(newLevel) + "_" + strconv.Itoa(intk) + "_metadata.db"}
//		return res
//	}
//}

/*
Potrebno je proslediti objekat klase SSTable i byte slice recorda da bi se formirali svi fajlovi za
taj konkretan sstable

func FormSSTable(sstable LSM.SSTable, recordElements []record.Record) {
}

Ista prica samo se objekat klase SSTable prosledjuje

func DeleteSSTable(sstable LSM.SSTable) {
	}
}
*/
