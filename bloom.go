package bloom

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/sohaha/zlsgo/zstring"
	"github.com/zlsgo/murmur3"
)

const (
	mod7       = 1<<3 - 1
	bitPerByte = 8
)

// Filter BloomFilter
type Filter struct {
	lock     *sync.RWMutex
	racelock bool
	m        uint64 // bit array of m bits, m will be ceiling to power of 2
	n        uint64 // number of inserted elements
	log2m    uint64 // log_2 of m
	k        uint64 // number of hash function
	keys     []byte // byte array to store hash value
}

func New(size uint64, k uint64, racelock bool) *Filter {
	log2 := uint64(math.Ceil(math.Log2(float64(size))))
	filter := &Filter{
		m:        1 << log2,
		log2m:    log2,
		k:        k,
		keys:     make([]byte, 1<<log2),
		racelock: racelock,
	}
	if filter.racelock {
		filter.lock = &sync.RWMutex{}
	}
	return filter
}

// Add adds byte array to bloom filter
func (f *Filter) Add(data []byte) *Filter {
	if f.racelock {
		f.lock.Lock()
		defer f.lock.Unlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		f.keys[slot] |= 1 << mod
	}
	f.n++
	return f
}

// Check check if byte array may exist in bloom filter
func (f *Filter) Check(data []byte) bool {
	if f.racelock {
		f.lock.RLock()
		defer f.lock.RUnlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		if f.keys[slot]&(1<<mod) == 0 {
			return false
		}
	}
	return true
}

// AddString adds string to filter
func (f *Filter) AddString(s string) *Filter {
	data := zstring.String2Bytes(s)
	return f.Add(data)
}

// CheckString if string may exist in filter
func (f *Filter) CheckString(s string) bool {
	data := zstring.String2Bytes(s)
	return f.Check(data)
}

// AddInt adds int to filter
func (f *Filter) AddInt(num int) *Filter {
	data := zstring.String2Bytes(strconv.Itoa(num))
	return f.Add(data)
}

// CheckInt checks if int is in filter
func (f *Filter) CheckInt(num int) bool {
	data := zstring.String2Bytes(strconv.Itoa(num))
	return f.Check(data)
}

// AddUInt16 adds uint16 to filter
func (f *Filter) AddUInt16(num uint16) *Filter {
	data := uint16ToBytes(num)
	return f.Add(data)
}

// CheckUInt16 checks if uint16 is in filter
func (f *Filter) CheckUInt16(num uint16) bool {
	data := uint16ToBytes(num)
	return f.Check(data)
}

// AddUInt32 adds uint32 to filter
func (f *Filter) AddUInt32(num uint32) *Filter {
	data := uint32ToBytes(num)
	return f.Add(data)
}

// CheckUInt32 checks if uint32 is in filter
func (f *Filter) CheckUInt32(num uint32) bool {
	data := uint32ToBytes(num)
	return f.Check(data)
}

// AddUInt64 adds uint64 to filter
func (f *Filter) AddUInt64(num uint64) *Filter {
	data := uint64ToBytes(num)
	return f.Add(data)
}

// CheckUInt64 checks if uint64 is in filter
func (f *Filter) CheckUInt64(num uint64) bool {
	data := uint64ToBytes(num)
	return f.Check(data)
}

// AddBatch add data array
func (f *Filter) AddBatch(dataArr [][]byte) *Filter {
	if f.racelock {
		f.lock.Lock()
		defer f.lock.Unlock()
	}
	for i := 0; i < len(dataArr); i++ {
		data := dataArr[i]
		h := baseHash(data)
		for i := uint64(0); i < f.k; i++ {
			loc := location(h, i)
			slot, mod := f.location(loc)
			f.keys[slot] |= 1 << mod
		}
		f.n++
	}
	return f
}

// AddIntBatch adds int array
func (f *Filter) AddIntBatch(numArr []int) *Filter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := zstring.String2Bytes(strconv.Itoa(numArr[i]))
		data = append(data, byteArr)
	}
	return f.AddBatch(data)
}

// AddUint16Batch adds uint16 array
func (f *Filter) AddUint16Batch(numArr []uint16) *Filter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint16ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return f.AddBatch(data)
}

// AddUint32Batch adds uint32 array
func (f *Filter) AddUint32Batch(numArr []uint32) *Filter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint32ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return f.AddBatch(data)
}

// AddUin64Batch  adds uint64 array
func (f *Filter) AddUin64Batch(numArr []uint64) *Filter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint64ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return f.AddBatch(data)
}

// location returns the bit position in byte array
// & (f.m - 1) is the quick way for mod operation
func (f *Filter) location(h uint64) (uint64, uint64) {
	slot := (h / bitPerByte) & (f.m - 1)
	mod := h & mod7
	return slot, mod
}

// location returns the ith hashed location using the four base hash values
func location(h []uint64, i uint64) uint64 {
	return h[i&1] + i*h[2+(((i+(i&1))&3)/2)]
}

// baseHash returns the murmur3 128-bit hash
func baseHash(data []byte) []uint64 {
	a1 := []byte{1}
	hasher := murmur3.New128()
	_, _ = hasher.Write(data)
	v1, v2 := hasher.Sum128()
	_, _ = hasher.Write(a1)
	v3, v4 := hasher.Sum128()
	return []uint64{
		v1, v2, v3, v4,
	}
}

// Reset reset the bits to zero used in filter
func (f *Filter) Reset() {
	if f.racelock {
		f.lock.Lock()
		defer f.lock.Unlock()
	}
	for i := 0; i < len(f.keys); i++ {
		f.keys[i] &= 0
	}
	f.n = 0
}

// Merge merges another filter into current one
func (f *Filter) Merge(g *Filter) error {
	if f.m != g.m {
		return fmt.Errorf("m's don't match: %d != %d", f.m, g.m)
	}

	if f.k != g.k {
		return fmt.Errorf("k's don't match: %d != %d", f.m, g.m)
	}
	if g.racelock {
		return errors.New("merging racelock filter is not support")
	}

	if f.racelock {
		f.lock.Lock()
		defer f.lock.Unlock()
	}
	for i := 0; i < len(f.keys); i++ {
		f.keys[i] |= g.keys[i]
	}
	return nil
}

// Cap return the size of bits
func (f *Filter) Cap() uint64 {
	if f.racelock {
		f.lock.RLock()
		defer f.lock.RUnlock()
	}
	return f.m
}

// Size return  count of inserted element
func (f *Filter) Size() uint64 {
	if f.racelock {
		f.lock.RLock()
		defer f.lock.RUnlock()
	}
	return f.n
}

// FPR FalsePositiveRate (1 - e^(-kn/m))^k
func (f *Filter) FPR() float64 {
	if f.racelock {
		f.lock.RLock()
		defer f.lock.RUnlock()
	}
	expoInner := -(float64)(f.k*f.n) / float64(f.m)
	rate := math.Pow(1-math.Pow(math.E, expoInner), float64(f.k))
	return rate
}
