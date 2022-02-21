package HLL

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
	"time"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M   uint32
	P   uint8
	Reg []uint8
	Ts uint
	hashFunction hash.Hash32
}

func CreateHLL(p uint8) *HLL{
	hll := &HLL{}
	if p > HLL_MAX_PRECISION || p < HLL_MIN_PRECISION {
		panic("Value of parameter p is not in acceptable range!")
	}
	hll.P = p
	hll.M = uint32(math.Pow(2, float64(hll.P)))
	hll.Reg = make([]uint8, hll.M, hll.M)
	hll.CreateHashFunction()
	return hll
}

func (hll *HLL) AddData(data string){
	hll.hashFunction.Reset()
	if _, err := hll.hashFunction.Write([]byte(data)); err != nil {
		panic(err)
	}
	bytes := hll.hashFunction.Sum32()
	offset := 32 - hll.P
	bucketIndex := bytes >> offset
	zeros := bits.TrailingZeros32(bytes)
	if hll.Reg[bucketIndex] < uint8(zeros) {
		hll.Reg[bucketIndex] = uint8(zeros)
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll* HLL) EncodeHLL(bytes []byte) {
	hll.M = uint32(binary.LittleEndian.Uint64(bytes[0:8]))
	hll.P = uint8(binary.LittleEndian.Uint64(bytes[8:16]))
	hll.Ts = uint(binary.LittleEndian.Uint64(bytes[16:24]))
	size := int(binary.LittleEndian.Uint64(bytes[24:32]))
	hll.Reg = make([]uint8, size)
	for i:=0; i<size; i++{
		hll.Reg[i] = uint8(binary.LittleEndian.Uint64(bytes[32+i*8:40+i*8]))
	}
	hll.CreateHashFunction()
}

func (hll* HLL) DecodeHLL() []byte{
	size:=len(hll.Reg)
	all := make([]byte, 0)
	bytes:=make([]byte, 8)

	binary.LittleEndian.PutUint64(bytes, uint64(hll.M))
	all = append(all, bytes...)

	binary.LittleEndian.PutUint64(bytes, uint64(hll.P))
	all = append(all, bytes...)

	binary.LittleEndian.PutUint64(bytes, uint64(hll.Ts))
	all = append(all, bytes...)

	binary.LittleEndian.PutUint64(bytes, uint64(size))
	all = append(all, bytes...)

	for i := 0; i < size; i++ {
		binary.LittleEndian.PutUint64(bytes, uint64(hll.Reg[i]))
		all = append(all, bytes...)
	}
	return all
}

func (hll *HLL) CreateHashFunction(){
	if hll.Ts == 0 {
		hll.Ts = uint(time.Now().Unix())
	}
	hll.hashFunction = murmur3.New32WithSeed(uint32(hll.Ts))
}
