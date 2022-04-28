package main

import (
	"Dandelion/skiplist"
	"Dandelion/sstable"
	"strconv"
)

func main() {
	skipList := skiplist.NewSkipList(32)
	for i := 17191; i < 871914; i++ {
		skipList.Put(i, []byte(strconv.Itoa(i*7)))
	}
	sstable.StorageData(skipList)
}
