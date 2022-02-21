package record

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type Record struct {
	Crc       uint32
	Timestamp int64
	Tombstone uint8
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func CreateRecord(key string, value []byte, delete byte) *Record {
	crc := CRC32(value)
	timestamp := time.Now().Unix()
	tombstone := delete
	keySize := uint64(len([]byte(key)))
	valueSize := uint64(len(value))
	return &Record{crc, timestamp, tombstone, keySize, valueSize, key, value}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (record *Record) GetSize() uint64 {
	return 4 + 8 + 1 + 8 + 8 + record.KeySize + record.ValueSize
}

func (record *Record) EncodeRecord() []byte {
	recordBytes := make([]byte, 0, record.GetSize())
	w := bytes.NewBuffer(recordBytes)

	err := binary.Write(w, binary.LittleEndian, record.Crc)
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, record.Timestamp)
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, record.Tombstone)
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, record.KeySize)
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, record.ValueSize)
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, []byte(record.Key))
	if err != nil {
		return nil
	}

	err = binary.Write(w, binary.LittleEndian, record.Value)
	if err != nil {
		return nil
	}

	return w.Bytes()
}

func (record *Record) DecodeRecord(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &record.Crc)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &record.Timestamp)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &record.Tombstone)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &record.KeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &record.ValueSize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	keyByteSlice := make([]byte, record.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	record.Key = string(keyByteSlice)

	record.Value = make([]byte, record.ValueSize)
	err = binary.Read(reader, binary.LittleEndian, &record.Value)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	return false
}

func (record *Record) Print() {
	fmt.Println("Crc:", record.Crc)
	fmt.Println("TimeStamp:", record.Timestamp)
	fmt.Println("Tombstone:", record.Tombstone)
	fmt.Println("Key size:", record.KeySize)
	fmt.Println("Value size:", record.ValueSize)
	fmt.Println("Key:", record.Key)
	fmt.Println("Value:", record.Value)
}
