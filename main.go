package main

import (
	"Dandelion/skiplist"
	"Dandelion/sstable"
	"math/rand"
	"strconv"
)

func main() {
	skipList := skiplist.NewSkipList(32)
	for i := 1; i < 11492; i++ {
		skipList.Put(i*11, []byte(strconv.Itoa(rand.Int())))
	}
	sstable.StorageData(skipList)
}
