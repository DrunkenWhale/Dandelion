package test

import (
	"Dandelion/skiplist"
	disk2 "Dandelion/sstable/disk"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestMerge(t *testing.T) {

	s1 := skiplist.NewSkipList(17)
	s2 := skiplist.NewSkipList(17)
	for i := 1; i < 117; i++ {
		s1.Put(i, []byte(strconv.Itoa(i*rand.Int())))
		s2.Put(i+57, []byte(strconv.Itoa(i*rand.Int())))
	}
	r := disk2.KVArrayMerge(s1.ExportAllElement(), s2.ExportAllElement())
	for _, e := range r {
		fmt.Println(e.Key, "==>", string(e.Value))
	}
	disk2.WriteDBFile("test", r)
}
