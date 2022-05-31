package filter

import (
	"bufio"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"strconv"
)

const (
	defaultBloomFilterSize = 1 << 20

	bloomFilterStorageFilePathPrefix = "data" + string(os.PathSeparator)
	bloomFilterStorageFileName       = "bloom_filter_data"

	defaultFileSep = '\n'

	defaultReadBuffer  = 4096 * 8
	defaultWriteBuffer = 4096 * 8

	defaultFreezeFilterElementSize = 50_000
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

func ClearBloomFilter(bloom *BloomFilter) error {
	exist := IsBloomFilterPersistenceExist()
	if exist {
		err := RemoveBloomFilterPersistenceFile()
		if err != nil {
			return err
		}
	}
	if bloom.elementMaxSize == defaultBloomFilterSize*7 {
		bloom = NewBloomFilter()
	} else {
		bloom = NewBloomFilterWithSize(bloom.elementMaxSize / 7)
	}
	return nil
}

func NewBloomFilterWithSize(elementMaxSize int) *BloomFilter {
	return &BloomFilter{
		elementMaxSize: elementMaxSize * 7,
		elementSize:    0,
		bitmap:         NewBitMap(elementMaxSize * 7),
		ln2hash:        int(math.Ceil(math.Ln2 * 3)),
	}
}

func GeneratorFilterFromFile() *BloomFilter {
	filter := NewBloomFilter()
	arr, err := filter.readBloomFilterDataFromFile()
	if err != nil {
		log.Fatalln(arr)
	}
	filter.bitmap.core = arr
	return filter
}

func IsBloomFilterPersistenceExist() bool {
	_, err := os.Stat(bloomFilterStorageFilePathPrefix + bloomFilterStorageFileName)
	if err != nil {
		return false
	} else {
		return true
	}
}

func RemoveBloomFilterPersistenceFile() error {
	err := os.Remove(bloomFilterStorageFilePathPrefix + bloomFilterStorageFileName)
	if err != nil {
		return err
	}
	return nil
}

func (filter *BloomFilter) Put(key int) {
	for i := 1; i <= filter.ln2hash; i++ {
		filter.bitmap.Put(getHashValue(key, i) % filter.elementMaxSize)
	}
	filter.elementSize++
	//dynamic expansion will cause a bug
	// hash value mod maxSize will be changed

	// every 50000 elements will cause a persistence
	// if number too smaller, write time will be too long
	if filter.elementSize%defaultFreezeFilterElementSize == 0 {
		err := filter.freezeBloomFilterDataToFile()
		if err != nil {
			log.Fatalln(err)
		}
	}
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

func (filter *BloomFilter) freezeBloomFilterDataToFile() error {

	file, err := os.OpenFile(bloomFilterStorageFilePathPrefix+bloomFilterStorageFileName, os.O_WRONLY|os.O_CREATE, 0777)

	if err != nil {
		return err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(file)

	buf := bufio.NewWriterSize(file, defaultWriteBuffer)
	for i, u := range filter.bitmap.core {
		_, err := buf.WriteString(strconv.FormatUint(u, 10))
		if err != nil {
			return err
		}
		err = buf.WriteByte(defaultFileSep)
		if err != nil {
			return err
		}
		if i%500 == 0 {
			err := buf.Flush()
			if err != nil {
				return err
			}
		}
	}
	err = buf.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (filter *BloomFilter) readBloomFilterDataFromFile() ([]uint64, error) {
	res := make([]uint64, 0)
	file, err := os.OpenFile(bloomFilterStorageFilePathPrefix+bloomFilterStorageFileName, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReaderSize(file, defaultReadBuffer)
	for {
		str, err := buf.ReadString(defaultFileSep)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		num, err := strconv.ParseUint(str[:len(str)-1], 10, 64)
		if err != nil {
			return nil, err
		}
		res = append(res, num)
	}
	return res, nil
}

func getHashValue(key int, hashNumber int) int {
	return int(
		crc32.ChecksumIEEE(
			[]byte(strconv.Itoa(
				int(crc32.ChecksumIEEE(
					[]byte(strconv.Itoa(key)))) +
					(hashNumber)))))
}
