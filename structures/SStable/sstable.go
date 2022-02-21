package SStable

import (
	"bufio"
	"encoding/binary"
	"napredni/structures/bloomFilter"
	"napredni/structures/merkleTree"
	"napredni/structures/record"
	"os"
)

const indexFileInterval = 10

type SSTable struct {
	DataFilePath string
	IndexFilePath string
	SummaryFilePath string
	FilterFilePath string
	MetadataFilePath string
	TOCFilePath string
}


// FormSSTable forms all necessary files and returns SSTable object for
// sstable based on passed paths and byte slice of records.
func FormSSTable(recordElements []record.Record, dataFilePath, indexFilePath, summaryFilePath,
	filterFilePath, metadataFilePath, tocFilePath string) *SSTable {
	formDataWithIndexAndSummary(recordElements, dataFilePath, indexFilePath, summaryFilePath)
	formFilter(recordElements, filterFilePath)
	formMetadata(recordElements, metadataFilePath)
	formTOC(dataFilePath, indexFilePath, summaryFilePath, filterFilePath, metadataFilePath, tocFilePath)

	return &SSTable{dataFilePath, indexFilePath, summaryFilePath,
		filterFilePath, metadataFilePath, tocFilePath}
}

// GetLevel returns level for SSTable object
func (sstable *SSTable) GetLevel() int {
	return getLevelForFileName(sstable.DataFilePath)
}

// GetRecordInSStableForKey returns record from data file
// and bool value that is true if record is found, otherwise false.
func (sstable *SSTable) GetRecordInSStableForKey(key string) (*record.Record, bool) {
	offsetIndexTable, found := getOffsetInIndexTableForKey(key, sstable.SummaryFilePath)
	if !found {
		return &record.Record{}, false
	}

	offsetDataTable, found := getOffsetInDataTableForKey(key, sstable.IndexFilePath, offsetIndexTable, indexFileInterval)
	if !found {
		return &record.Record{}, false
	}

	foundRecord, found := getRecordInDataTableForOffset(sstable.DataFilePath, offsetDataTable)
	if !found {
		return &record.Record{}, false
	}

	return foundRecord, true
}

// GetRecordsFromDataFile reads record slice from data file
func (sstable *SSTable) GetRecordsFromDataFile() []record.Record {
	file, err := os.OpenFile(sstable.DataFilePath, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)

	records := make([]record.Record, 0)
	recordToRead := record.Record{}
	for {
		eof := recordToRead.DecodeRecord(reader)
		if eof {
			return records
		}
		records = append(records, recordToRead)
	}
}

// DeleteSSTable deletes all files related to sstable based
// on passed object SSTable
func (sstable *SSTable) DeleteSSTable() {
	err := os.Remove(sstable.DataFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(sstable.IndexFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(sstable.SummaryFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(sstable.FilterFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(sstable.MetadataFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(sstable.TOCFilePath)
	if err!= nil {
		panic(err)
	}
}

// formDataWithIndexAndSummary forms index and summary file based on record elements passed to the function.
func formDataWithIndexAndSummary(recordElements []record.Record, dataFilePath, indexFilePath, summaryFilePath string) {
	dataFile, err := os.OpenFile(dataFilePath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	dataFileWriter := bufio.NewWriter(dataFile)

	indexFile, err := os.OpenFile(indexFilePath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	indexFileWriter := bufio.NewWriter(indexFile)

	summaryFile, err := os.OpenFile(summaryFilePath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	summaryFileWriter := bufio.NewWriter(summaryFile)

	// Initialization of header in summary file. We know that
	// first key will be min and last max because the slice
	// is sorted.
	summaryHeader := SummaryTableHeader{}
	summaryHeader.MinKey = recordElements[0].Key
	summaryHeader.MinKeySize = uint64(len(summaryHeader.MinKey))
	summaryHeader.MaxKey = recordElements[len(recordElements)-1].Key
	summaryHeader.MaxKeySize = uint64(len(summaryHeader.MaxKey))

	summaryEntries := make([]SummaryTableEntry, 0)

	offsetInDataFile := uint64(0)
	offsetInIndexFile := uint64(0)

	for index, recordElement := range recordElements {
		// Writing record to data file
		WriteRecordToDataFile(&recordElement, dataFileWriter)

		// Forming index entry and writing it to the index file.
		indexEntry := IndexTableEntry{KeySize: uint64(len(recordElement.Key)),
			Key: recordElement.Key, Offset: offsetInDataFile}
		indexEntry.WriteEntryToIndexFile(indexFileWriter)
		offsetInDataFile += recordElement.GetSize()

		// indexFileInterval represents distance between
		// 2 index entries in index file that are written
		// in summary file.
		if index%indexFileInterval == 0 || index == len(recordElements)-1 {
			summaryEntry := SummaryTableEntry{KeySize: indexEntry.KeySize, Key: indexEntry.Key,
				Offset: offsetInIndexFile}
			summaryEntries = append(summaryEntries, summaryEntry)

			summaryHeader.EntriesSize += summaryEntry.GetSize()
		}
		offsetInIndexFile += indexEntry.GetSize()
	}

	// After EntriesSize in summaryHeader is calculated,
	// summaryHeader and all summary entries are written
	// to summary file.
	summaryHeader.WriteHeaderToSummaryFile(summaryFileWriter)
	for _, summaryEntry := range summaryEntries {
		summaryEntry.WriteEntryToSummaryFile(summaryFileWriter)
	}

	dataFileWriter.Flush()
	dataFile.Close()
	indexFileWriter.Flush()
	indexFile.Close()
	summaryFileWriter.Flush()
	summaryFile.Close()
}

// Function that forms bloom filter based on byte slice of records
// and file path that is passed.
func formFilter(recordElements []record.Record, filterFilePath string) {
	filter := bloomFilter.CreateBloomFilter(len(recordElements), 0.01)
	for _, recordElement := range recordElements {
		filter.AddData(recordElement.Key)
	}
	filter.EncodeBloomFilter(filterFilePath)
}

// Function that forms metadata based on byte slice of records
// and file path that is passed.
func formMetadata(recordElements []record.Record, metadataFilePath string) {
	recordElementsBytes :=  make([][]byte, 0)
	for _, recordElement := range recordElements {
		recordElementBytes := recordElement.EncodeRecord()
		recordElementsBytes = append(recordElementsBytes, recordElementBytes)
	}

	metadata := merkleTree.MerkleTree{}
	metadata.Build(recordElementsBytes)
	metadata.Serialize(metadataFilePath)
}


// Function that forms table of content based on file
// paths passed to the function.
func formTOC(dataFilePath, indexFilePath, summaryFilePath, filterFilePath, metadataFilePath, TOCFilePath string) {
	tocFile, err := os.OpenFile(TOCFilePath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	tocFileWriter := bufio.NewWriter(tocFile)

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(dataFilePath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(indexFilePath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(summaryFilePath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(filterFilePath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(metadataFilePath + "\n"))
	if err != nil {
		return
	}

	tocFileWriter.Flush()
	tocFile.Close()
}