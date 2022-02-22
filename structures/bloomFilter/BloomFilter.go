package bloomFilter

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"log"
	"math"
	"os"
	"time"
)

type BloomFilter struct{
	M uint
	K uint
	Ts uint
	hashFunctions []hash.Hash32
	Bits []int
}

func CreateBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter{
	bf := &BloomFilter{}
	bf.M = CalculateM(expectedElements, falsePositiveRate)
	bf.K = CalculateK(expectedElements, bf.M)
	bf.CreateHashFunctions()
	bf.Bits = make([]int, bf.M, bf.M)
	return bf
}

func (bf *BloomFilter) CreateHashFunctions() {
	h := []hash.Hash32{}
	if bf.Ts == 0 {
		bf.Ts = uint(time.Now().Unix())
	}
	for i := uint(0); i < bf.K; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(bf.Ts+1)))
	}
	bf.hashFunctions = h
}

func (bf *BloomFilter) AddData(data string){
	for i:=0; i < int(bf.K); i++{
		bf.hashFunctions[i].Reset()
		if _, err := bf.hashFunctions[i].Write([]byte(data)); err != nil{
			panic(err)
		}
		index := bf.hashFunctions[i].Sum32() % uint32(bf.M)
		bf.Bits[index] = 1
	}
}

func (bf *BloomFilter) FindData(data string) bool{
	for i := 0; i < int(bf.K); i++{
		bf.hashFunctions[i].Reset()
		if _, err := bf.hashFunctions[i].Write([]byte(data)); err != nil{
			panic(err)
		}
		index := bf.hashFunctions[i].Sum32() % uint32(bf.M)
		if bf.Bits[index] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) EncodeBloomFilter(filterFilePath string) {
	file, err := os.Create(filterFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(&bf); err != nil {
		panic(err)
	}
}

func (bf *BloomFilter) DecodeBloomFilter(filterFilePath string) {
	file, err := os.OpenFile(filterFilePath, os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(&bf); err != nil{
		panic(err)
	}
	bf.CreateHashFunctions()
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}
