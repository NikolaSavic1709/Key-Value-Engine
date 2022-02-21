package SStable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// SummaryTableHeader contains min and max key from index file, and it's sizes.
type SummaryTableHeader struct {
	MinKeySize  uint64
	MinKey      string
	MaxKeySize  uint64
	MaxKey      string
	EntriesSize uint64
}

// SummaryTableEntry contains key, key size, and it's offset in the index file.
type SummaryTableEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

// Print prints a summary header info to terminal.
func (summaryHeader *SummaryTableHeader) Print() {
	fmt.Println("Min key size:", summaryHeader.MinKeySize)
	fmt.Println("Min key:", summaryHeader.MinKey)
	fmt.Println("Max key size:", summaryHeader.MaxKeySize)
	fmt.Println("Max key:", summaryHeader.MaxKey)
	fmt.Println("Summary entries size:", summaryHeader.EntriesSize)
}

// Print prints a summary entry info to terminal.
func (summaryEntry *SummaryTableEntry) Print() {
	fmt.Println("Key size:", summaryEntry.KeySize)
	fmt.Println("Key:", summaryEntry.Key)
	fmt.Println("Offset:", summaryEntry.Offset)
}

// GetSize returns size in bytes of a single summary entry.
func (summaryEntry *SummaryTableEntry) GetSize() uint64 {
	return 8 + summaryEntry.KeySize + 8
}

// WriteHeaderToSummaryFile writes a summary header to file.
func (summaryHeader *SummaryTableHeader) WriteHeaderToSummaryFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryHeader.MinKeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MinKey))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.MaxKeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MaxKey))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.EntriesSize)
	if err != nil {
		panic(err)
	}
}

// ReadHeaderFromSummaryFile reads a header from file.
func (summaryHeader *SummaryTableHeader) ReadHeaderFromSummaryFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &summaryHeader.MinKeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	minKeyByteSlice := make([]byte, summaryHeader.MinKeySize)
	err = binary.Read(reader, binary.LittleEndian, &minKeyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	summaryHeader.MinKey = string(minKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.MaxKeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	maxKeyByteSlice := make([]byte, summaryHeader.MaxKeySize)
	err = binary.Read(reader, binary.LittleEndian, &maxKeyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	summaryHeader.MaxKey = string(maxKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.EntriesSize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	return false
}

// WriteEntryToSummaryFile writes a summary entry to file.
func (summaryEntry *SummaryTableEntry) WriteEntryToSummaryFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryEntry.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryEntry.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryEntry.Offset)
	if err != nil {
		panic(err)
	}
}

// ReadEntryFromSummaryFile reads an entry from file.
func (summaryEntry *SummaryTableEntry) ReadEntryFromSummaryFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &summaryEntry.KeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	keyByteSlice := make([]byte, summaryEntry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	summaryEntry.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryEntry.Offset)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	return false
}

// GetOffsetInIndexTableForKey returns offset for index table for passed key and
// bool value that is true if offset is found, otherwise false.
func getOffsetInIndexTableForKey(key string, filePath string) (uint64, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	summaryHeader := SummaryTableHeader{}
	eof := summaryHeader.ReadHeaderFromSummaryFile(reader)
	if eof {
		return 0, false
	}

	if summaryHeader.MinKey > key {
		return 0, false
	}

	if summaryHeader.MaxKey < key {
		return 0, false
	}

	buf := make([]byte, summaryHeader.EntriesSize)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		panic(err)
	}

	reader = bufio.NewReader(bytes.NewBuffer(buf))
	prevSummaryEntry := SummaryTableEntry{}
	nextSummaryEntry := SummaryTableEntry{}

	for {
		prevSummaryEntry = nextSummaryEntry
		eof = nextSummaryEntry.ReadEntryFromSummaryFile(reader)
		if eof {
			return prevSummaryEntry.Offset, true
		}

		if prevSummaryEntry.Key <= key && key < nextSummaryEntry.Key {
			break
		}
	}

	return prevSummaryEntry.Offset, true
}

func (sstable *SSTable) PrintSummaryFile() {
	file, err := os.Open(sstable.SummaryFilePath)
	if err!= nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	summaryHeader := SummaryTableHeader{}
	eof := summaryHeader.ReadHeaderFromSummaryFile(reader)
	if eof {
		return
	}

	fmt.Println("****************************Header****************************")
	summaryHeader.Print()

	fmt.Println()
	fmt.Println("****************************Entries****************************")
	i := 1
	summaryEntry := SummaryTableEntry{}
	for {
		eof := summaryEntry.ReadEntryFromSummaryFile(reader)
		if eof {
			return
		}

		fmt.Println("Entry", i)
		summaryEntry.Print()
		fmt.Println()
		i++
	}
}
