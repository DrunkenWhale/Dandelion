package filter

import (
	"hash/crc32"
	"math"
	"strconv"
)

const (
	defaultBloomFilterSize = 1 << 20
)

type BloomFilter struct {
	elementMaxSize int
	elementSize    int
	bitmap         *BitMap
	ln2hash        int
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		elementMaxSize: defaultBloomFilterSize * 7,
		elementSize:    0,
		bitmap:         NewBitMap(defaultBloomFilterSize * 7),
		ln2hash:        3,
	}
}

func NewBloomFilterWithSize(elementMaxSize int) *BloomFilter {
	return &BloomFilter{
		elementMaxSize: elementMaxSize * 7,
		elementSize:    0,
		bitmap:         NewBitMap(elementMaxSize * 7),
		ln2hash:        int(math.Ceil(math.Ln2 * 3)),
	}
}

func (filter *BloomFilter) Put(key int) {
	for i := 1; i <= filter.ln2hash; i++ {
		filter.bitmap.Put(getHashValue(key, i) % filter.elementMaxSize)
	}
	filter.elementSize++

	//dynamic expansion will cause a bug
	// hash value mod maxSize will be changed

	//expansion bloom_filter
	//if float32(1.0*filter.elementMaxSize/filter.elementSize) < 1.7 {
	//	filter.bitmap = filter.bitmap.ExpansionBitMap()
	//	filter.elementMaxSize *= 5
	//	filter.ln2hash *= 5
	//}
}

// Get
//return true : key exist
//return false: key unexist
func (filter *BloomFilter) Get(key int) bool {
	for i := 1; i <= filter.ln2hash; i++ {
		if !filter.bitmap.Get(getHashValue(key, i) % filter.elementMaxSize) {
			return false
		}
	}
	return true
}

func getHashValue(key int, hashNumber int) int {
	return int(
		crc32.ChecksumIEEE(
			[]byte(strconv.Itoa(
				int(crc32.ChecksumIEEE(
					[]byte(strconv.Itoa(key)))) +
					(hashNumber)))))
}
