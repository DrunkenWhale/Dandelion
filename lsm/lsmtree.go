package lsm

import (
	"Dandelion/filter"
	"Dandelion/sstable"
)

type LSM struct {
	table *sstable.SSTable
	bloom *filter.BloomFilter
}

func NewLSM() *LSM {
	return &LSM{
		table: sstable.NewSSTable(),
		bloom: filter.NewBloomFilter(),
	}
}

func (lsm *LSM) Get(key int) ([]byte, bool) {
	if lsm.bloom.Get(key) {
		return lsm.table.Get(key)
	} else {
		// can't restart
		return nil, false
	}
}

func (lsm *LSM) Put(key int, value []byte) error {
	lsm.bloom.Put(key)
	return lsm.table.Put(key, value)
}

func (lsm *LSM) Update(key int, value []byte) error {
	lsm.bloom.Put(key)
	return lsm.table.Update(key, value)
}

func (lsm *LSM) Delete(key int) error {
	return lsm.table.Delete(key)
}
