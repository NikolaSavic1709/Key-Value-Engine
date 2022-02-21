package SStable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// IndexTableEntry forms inside an index file.
type IndexTableEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

// GetSize returns size in bytes of a single index entry.
func (indexEntry *IndexTableEntry) GetSize() uint64 {
	return 8 + indexEntry.KeySize + 8
}

// Print prints an entry info to terminal.
func (indexEntry *IndexTableEntry) Print() {
	fmt.Println("Key size:", indexEntry.KeySize)
	fmt.Println("Key:", indexEntry.Key)
	fmt.Println("Offset:", indexEntry.Offset)
}

// WriteEntryToIndexFile writes a single index entry to file.
func (indexEntry *IndexTableEntry) WriteEntryToIndexFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, indexEntry.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(indexEntry.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, indexEntry.Offset)
	if err != nil {
		panic(err)
	}
}

// ReadEntryFromIndexFile reads a single index entry from file to passed pointer
func (indexEntry *IndexTableEntry) ReadEntryFromIndexFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &indexEntry.KeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	keyByteSlice := make([]byte, indexEntry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	indexEntry.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &indexEntry.Offset)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	return false
}

// GetOffsetInDataTableForKey returns offset for data table for passed key and
// bool value that is true if offset is found, otherwise false.
func getOffsetInDataTableForKey(key string, filePath string, offset uint64, intervalSize uint64) (uint64, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
		return 0, false
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return 0, false
	}

	tmpIndexEntry := IndexTableEntry{}
	for i := uint64(0); i < intervalSize; i++ {
		eof := tmpIndexEntry.ReadEntryFromIndexFile(reader)
		if eof {
			return 0, false
		}

		if tmpIndexEntry.Key == key {
			return tmpIndexEntry.Offset, true
		}
	}
	return 0, false
}

func (sstable *SSTable) PrintIndexFile() {
	file, err := os.Open(sstable.IndexFilePath)
	if err!= nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	fmt.Println("****************************Entries****************************")
	i := 1
	indexEntry := IndexTableEntry{}
	for {
		eof := indexEntry.ReadEntryFromIndexFile(reader)
		if eof {
			break
		}

		fmt.Println("Entry", i)
		indexEntry.Print()
		fmt.Println()
		i++
	}
}
