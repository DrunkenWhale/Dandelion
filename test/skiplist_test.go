package test

import (
	"Dandelion/skiplist"
	"testing"
	"time"
)

func TestSkipListPrintMethod(t *testing.T) {
	skipList := skiplist.NewSkipList(77)
	skipList.Insert(778, "sda")
	skipList.Insert(7738, 1)
	skipList.Insert(72178, true)
	skipList.Insert(71378, 114)
	skipList.Insert(72378, "114")
	skipList.Insert(73478, "???")
	skipList.Insert(77568, "&&&")
	skipList.PrintSkipList()
}

func TestOperationTimeBySkipList(t *testing.T) {
	// 性能低到离谱的原因很有可能是因为层高过低...
	skipList := skiplist.NewSkipList(77)
	t2 := time.Now().Unix()
	for i := 0; i < 11451419; i += 1 {
		skipList.Insert(i, 114514)
	}

	t.Log(time.Now().Unix() - t2)
	m := make(map[int]interface{})
	t1 := time.Now().Unix()
	for i := 0; i < 11451419; i++ {
		m[i] = 114514
	}
	t.Log(time.Now().Unix() - t1)
}
