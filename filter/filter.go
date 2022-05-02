package filter

import (
	"hash/adler32"
	"math"
	"strconv"
)

const (
	defaultBloomFilter = 1 << 12
)

type BloomFilter struct {
	elementMaxSize int
	elementSize    int
	bitmap         *BitMap
	ln2hash        int
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		elementMaxSize: defaultBloomFilter * 7,
		elementSize:    0,
		bitmap:         NewBitMap(defaultBloomFilter * 7),
		ln2hash:        2,
	}
}

func NewBloomFilterWithSize(elementMaxSize int) *BloomFilter {
	return &BloomFilter{
		elementMaxSize: elementMaxSize * 7,
		elementSize:    0,
		bitmap:         NewBitMap(elementMaxSize * 7),
		ln2hash:        int(math.Floor(math.Ln2 * 3)),
	}
}

func (filter *BloomFilter) Put(key int) {
	for i := 0; i < filter.ln2hash; i++ {
		filter.bitmap.Put(getHashValue(key, i))
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
	for i := 0; i < filter.ln2hash; i++ {
		if filter.bitmap.Get(getHashValue(key, i) % filter.elementMaxSize) {
			return true
		}
	}
	return false
}

func getHashValue(key int, hashNumber int) int {
	return int(
		adler32.Checksum(
			[]byte(strconv.Itoa(
				int(adler32.Checksum(
					[]byte(strconv.Itoa(key)))) +
					(hashNumber)))))
}
