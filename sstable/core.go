package sstable

import (
	"github.com/Pigeon377/Dandelion/skiplist"
	"log"
	"sync"
	"time"
)

const (

	// increase defaultMemorySize make SSTable has less persistence operation
	// and write more data once
	// decrease I/O consume
	defaultMemorySize = 4096 * 8 * 8

	defaultSkipListHeight = 32

	putOperation byte = 0

	deleteOperation byte = 1

	autoFlushTimeInterval = 7 * time.Second
)

type SSTable struct {
	mutex sync.Mutex

	skipList       *skiplist.SkipList
	maxMemorySize  int
	skipListHeight int
}

func NewSSTable() *SSTable {

	sstable := &SSTable{
		skipList:       skiplist.NewSkipList(defaultSkipListHeight),
		maxMemorySize:  defaultMemorySize,
		skipListHeight: defaultSkipListHeight,
	}
	// this function should be implemented in high level abstraction
	// shouldn't be implemented in lsm/sstable
	// and user should choose whether auto flush
	//go func() {
	//	ticker := time.NewTicker(autoFlushTimeInterval)
	//	for range ticker.C {
	//		err := sstable.Flush()
	//		if err != nil {
	//			log.Fatalln(err)
	//		}
	//	}
	//}()

	return sstable
}

func NewSSTableWithMemorySize(memorySize int) *SSTable {
	sstable := &SSTable{
		skipList:       skiplist.NewSkipList(defaultSkipListHeight),
		maxMemorySize:  memorySize,
		skipListHeight: defaultSkipListHeight,
	}
	//go func() {
	//	ticker := time.NewTicker(autoFlushTimeInterval)
	//	for range ticker.C {
	//		err := sstable.Flush()
	//		if err != nil {
	//			log.Fatalln(err)
	//		}
	//	}
	//}()
	return sstable
}

func NewSSTableWithSkipListHeight(skipListHeight int) *SSTable {
	sstable := &SSTable{
		skipList:       skiplist.NewSkipList(defaultSkipListHeight),
		maxMemorySize:  defaultMemorySize,
		skipListHeight: skipListHeight,
	}
	//go func() {
	//	ticker := time.NewTicker(autoFlushTimeInterval)
	//	for range ticker.C {
	//		err := sstable.Flush()
	//		if err != nil {
	//			log.Fatalln(err)
	//		}
	//	}
	//}()

	return sstable
}

func NewSSTableWithMemorySizeAndSkipListHeight(memorySize int, skipListHeight int) *SSTable {
	sstable := &SSTable{
		skipList:       skiplist.NewSkipList(defaultSkipListHeight),
		maxMemorySize:  memorySize,
		skipListHeight: skipListHeight,
	}

	//go func() {
	//	ticker := time.NewTicker(autoFlushTimeInterval)
	//	for range ticker.C {
	//		err := sstable.Flush()
	//		if err != nil {
	//			log.Fatalln(err)
	//		}
	//	}
	//}()

	return sstable
}

func (table *SSTable) Get(key int) ([]byte, bool) {
	bytes, ok, err := table.search(key)
	if err != nil {
		// program error
		// shouldn't exist in truly operation
		log.Println(err)
	}

	if !ok {
		return nil, false
	}

	// bytes will not be nil
	// because it has an operation byte in its head

	if bytes[0] == putOperation {

		return bytes[1:], true

	} else if bytes[0] == deleteOperation {

		return nil, false

	} else {

		log.Fatalln("Illegal Operation Code")
		return nil, false

	}
}

func (table *SSTable) Put(key int, value []byte) error {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	v := append(
		[]byte{
			putOperation,
		}, value...)
	table.skipList.Put(key, v)
	if table.skipList.MemorySize() > defaultMemorySize {
		// storage data to disk
		err := freezeDataToFile(table.skipList)
		if err != nil {
			return err
		}
		// needn't clear skiplist
		//table.skipList = skiplist.NewSkipList(defaultSkipListHeight)
	}
	return nil
}

func (table *SSTable) Update(key int, value []byte) error {
	err := table.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (table *SSTable) Delete(key int) error {
	table.skipList.Put(key, []byte{deleteOperation})
	if table.skipList.MemorySize() > defaultMemorySize {
		err := freezeDataToFile(table.skipList)
		if err != nil {
			return err
		}
		table.skipList = skiplist.NewSkipList(defaultSkipListHeight)
	}
	return nil
}

func (table *SSTable) search(key int) ([]byte, bool, error) {
	bytes, ok := table.skipList.Get(key)
	if ok {
		return bytes, true, nil
	} else {
		return table.searchFromFile(key)
	}
}

func (table *SSTable) searchFromFile(key int) ([]byte, bool, error) {
	return searchKeyFromFile(key)
}

func (table *SSTable) Flush() error {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	err := freezeDataToFile(table.skipList)
	if err != nil {
		return err
	}
	return nil
}

// ClearMemory
// clear skiplist and flush data to disk
func (table *SSTable) ClearMemory() error {
	err := freezeDataToFile(table.skipList)
	if err != nil {
		return err
	}
	table.skipList = skiplist.NewSkipList(defaultSkipListHeight)
	return nil
}
