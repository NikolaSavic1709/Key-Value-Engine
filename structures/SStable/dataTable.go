package SStable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"napredni/structures/record"
	"os"
)

// WriteRecordToDataFile writes a data record to file.
func WriteRecordToDataFile(record *record.Record, writer *bufio.Writer) {
	recordByteSlice := record.EncodeRecord()

	err := binary.Write(writer, binary.LittleEndian, recordByteSlice)
	if err != nil {
		return
	}
}

// ReadRecordFromDataFile reads a record from file
func ReadRecordFromDataFile(record *record.Record, reader *bufio.Reader) bool {
	eof := record.DecodeRecord(reader)

	return eof
}

// GetRecordInDataTableForOffset reads record at specified offset.
func getRecordInDataTableForOffset(filePath string, offset uint64) (*record.Record, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)

	file.Seek(int64(offset), 0)

	foundRecord := record.Record{}
	eof := ReadRecordFromDataFile(&foundRecord, reader)
	if eof {
		return &record.Record{}, false
	}
	return &foundRecord, true
}

func (sstable *SSTable) PrintDataFile() {
	file, err := os.Open(sstable.DataFilePath)
	if err!= nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	fmt.Println("****************************Records****************************")
	i := 1
	recordToPrint := record.Record{}
	for {
		eof := recordToPrint.DecodeRecord(reader)
		if eof {
			break
		}

		fmt.Println("Record", i)
		recordToPrint.Print()
		fmt.Println()
		i++
	}
}
