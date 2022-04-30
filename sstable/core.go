package sstable

import "Dandelion/skiplist"

const defaultMemorySize = 4096 * 68
const defaultSkipListHeight = 32

type SSTable struct {
	skipList      *skiplist.SkipList
	maxMemorySize int
}

func NewSSTable() *SSTable {
	return &SSTable{
		skipList:      skiplist.NewSkipList(defaultSkipListHeight),
		maxMemorySize: defaultMemorySize,
	}
}

func (table *SSTable) Get(key int) {

}

func (table *SSTable) Put(key int, value []byte) error {
	table.skipList.Put(key, value)
	if table.skipList.MemorySize() > defaultMemorySize {
		err := FreezeDataToFile(table.skipList)
		if err != nil {
			return err
		}
		table.skipList = skiplist.NewSkipList(defaultSkipListHeight)
	}
	return nil
}

func (table *SSTable) Update(key int, value []byte) {
	table.Put(key, value)
}

func (table *SSTable) Delete(key int) {

}

func (table *SSTable) search(key int) {

}

func (table *SSTable) searchFromFile(key int, filename string) {

}
