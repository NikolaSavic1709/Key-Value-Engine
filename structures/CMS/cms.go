package CMS

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct {
	M             uint32
	K             uint32
	Ts            uint32
	hashFunctions []hash.Hash32
	Matrix        [][]uint32
}

func CreateCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	cms := &CountMinSketch{}
	cms.M = CalculateM(epsilon)
	cms.K = CalculateK(delta)
	cms.CreateHashFunctions()
	cms.Matrix = make([][]uint32, cms.M)
	for i := range cms.Matrix {
		cms.Matrix[i] = make([]uint32, cms.M)
	}
	return cms
}

func (cms *CountMinSketch) CreateHashFunctions() {
	h := []hash.Hash32{}
	if cms.Ts == 0 {
		cms.Ts = uint32(time.Now().Unix())
	}
	for i := uint32(0); i < cms.K; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(cms.Ts+1)))
	}
	cms.hashFunctions = h
}

func (cms *CountMinSketch) AddData(data string) {
	for i := 0; i < int(cms.K); i++ {
		cms.hashFunctions[i].Reset()
		if _, err := cms.hashFunctions[i].Write([]byte(data)); err != nil {
			panic(err)
		}
		col := cms.hashFunctions[i].Sum32() % uint32(cms.M)
		cms.Matrix[i][col] += 1
	}
}

func (cms *CountMinSketch) FindDataFrequency(data string) uint32 {
	rows := make([]uint32, cms.K, cms.K)
	for i := 0; i < int(cms.K); i++ {
		cms.hashFunctions[i].Reset()
		if _, err := cms.hashFunctions[i].Write([]byte(data)); err != nil {
			panic(err)
		}
		col := cms.hashFunctions[i].Sum32() % uint32(cms.M)
		rows[i] = cms.Matrix[i][col]
	}
	return Min(rows)
}

func (cms *CountMinSketch) EncodeCMS(bytes []byte) {
	cms.M = uint32(binary.LittleEndian.Uint64(bytes[0:8]))
	cms.K = uint32(binary.LittleEndian.Uint64(bytes[8:16]))
	cms.Ts = uint32(binary.LittleEndian.Uint64(bytes[16:24]))
	n := int(binary.LittleEndian.Uint64(bytes[24:32]))
	m := int(binary.LittleEndian.Uint64(bytes[32:40]))
	cms.Matrix = make([][]uint32, n)
	for i := 0; i < n; i++ {
		cms.Matrix[i] = make([]uint32, m)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			cms.Matrix[i][j] = uint32(binary.LittleEndian.Uint64(bytes[40+i*m*8+j*8 : 48+i*m*8+j*8]))
			binary.LittleEndian.PutUint64(bytes, uint64(cms.Matrix[i][j]))
		}
	}
	cms.CreateHashFunctions()
}

func (cms *CountMinSketch) DecodeCMS() []byte {
	n := len(cms.Matrix)
	m := len(cms.Matrix[0])
	all := make([]byte, 0)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(cms.M))
	all = append(all, bytes...)
	binary.LittleEndian.PutUint64(bytes, uint64(cms.K))
	all = append(all, bytes...)
	binary.LittleEndian.PutUint64(bytes, uint64(cms.Ts))
	all = append(all, bytes...)
	binary.LittleEndian.PutUint64(bytes, uint64(n))
	all = append(all, bytes...)
	binary.LittleEndian.PutUint64(bytes, uint64(m))
	all = append(all, bytes...)
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			binary.LittleEndian.PutUint64(bytes, uint64(cms.Matrix[i][j]))
			all = append(all, bytes...)
		}
	}
	return all
}

func CalculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}

func Min(rows []uint32) uint32 {
	min := rows[0]
	for _, v := range rows {
		if min > v {
			min = v
		}
	}
	return min
}

