package WAL

import (
	"bufio"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"log"
	"napredni/structures/record"
	"os"
	"strconv"
)

type WAL struct {
	LastSegmentPath string
	SegmentPaths    []string
	DirPath         string
	Lwm             uint8
	MaxNumOfRecords uint8
	NumOfRecords    uint8
}

func CreateWAL(path string, lwm uint8, maxRecords uint8) (*WAL, error) {
	var filePath string
	paths := make([]string, 0)

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	numOfFiles := len(files)
	if numOfFiles == 0 {
		filePath = path + "wal_1.bin"
		paths = append(paths, filePath)
		numOfFiles++
		file, err := os.OpenFile(filePath, os.O_CREATE, 0777)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

	} else {
		filePath = path + files[numOfFiles-1].Name()
		for i := 0; i < numOfFiles; i++ {
			paths = append(paths, path+files[i].Name())
		}
	}

	wal := &WAL{
		LastSegmentPath: filePath,
		SegmentPaths:    paths,
		DirPath:         path,
		Lwm:             lwm,
		MaxNumOfRecords: maxRecords,
	}
	wal.NumOfRecords = uint8(wal.CalculateNumOfRecords())
	return wal, nil
}

func (wal *WAL) Update() {
	filePath := wal.DirPath + "wal_1.bin"
	paths := make([]string, 0)
	paths = append(paths, filePath)
	file, err := os.OpenFile(filePath, os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	wal.LastSegmentPath = filePath
	wal.SegmentPaths = paths
	wal.NumOfRecords = 0
}

func (wal *WAL) AddData(key string, value []byte) bool {
	return wal.AppendData(key, value, 0)
}

func (wal *WAL) DeleteData(key string, value []byte) bool {
	return wal.AppendData(key, value, 1)
}

func (wal *WAL) AppendData(key string, value []byte, del byte) bool {
	if wal.NumOfRecords == wal.MaxNumOfRecords {
		wal.NumOfRecords = 0
		newPath := wal.DirPath + "wal_" + strconv.Itoa(len(wal.SegmentPaths)+1) + ".bin"
		wal.SegmentPaths = append(wal.SegmentPaths, newPath)
		wal.LastSegmentPath = newPath
		_, err := os.Create(newPath)
		if err != nil {
			return false
		}
	}

	newRecord := record.CreateRecord(key, value, del)
	recordBytes := newRecord.EncodeRecord()
	file, err := os.OpenFile(wal.LastSegmentPath, os.O_RDWR, 0777)
	if err != nil {
		return false
	}
	defer file.Close()
	err = appendWAL(file, recordBytes)
	if err != nil {
		return false
	}
	wal.NumOfRecords++
	return true
}

func (wal *WAL) CalculateNumOfRecords() int {
	numOfRecords := 0
	file, err := os.OpenFile(wal.LastSegmentPath, os.O_RDWR, 0777)
	if err != nil {
		return -1
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	record := record.Record{}
	for {
		if record.DecodeRecord(reader) {
			break
		}
		numOfRecords++
	}
	return numOfRecords
}

func (wal *WAL) DeleteSegments(pathsForDelete []string){
	for i := 0; i < len(pathsForDelete); i++ {
		err := os.Remove(wal.SegmentPaths[i])
		if err != nil {
			panic(err)
		}
	}
	files, err := ioutil.ReadDir(wal.DirPath)
	paths := make([]string, 0)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(files); i++ {
		newPath := wal.DirPath + "wal_" + strconv.Itoa(i+1) + ".bin"
		paths = append(paths, newPath)
		err := os.Rename(wal.DirPath+files[i].Name(), newPath)
		if err != nil {
			panic(err)
		}
	}
	wal.SegmentPaths = paths
}

func (wal *WAL) DeleteAllSegments() {
	for i := 0; i < len(wal.SegmentPaths); i++ {
		err := os.Remove(wal.SegmentPaths[i])
		if err != nil {
			panic(err)
		}
	}
	wal.Update()
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func appendWAL(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	//mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	return nil
}