package test

import (
	"Dandelion/skiplist"
	"testing"
	"time"
)

func TestSkipListGetMethod(t *testing.T) {
	skipList := skiplist.NewSkipList(17)
	skipList.Put(778, []byte("sda"))
	skipList.Put(7738, []byte("1"))
	skipList.Put(72178, []byte("true"))
	skipList.Put(71378, []byte("114"))
	skipList.Put(72378, []byte("114"))
	skipList.Put(73478, []byte("???"))
	skipList.Put(77568, []byte("&&&"))
	t.Log(skipList.Get(778))
	t.Log(skipList.Get(7738))
	t.Log(skipList.Get(71378))
	t.Log(skipList.Get(72378))
	t.Log(skipList.Get(73478))
	t.Log(skipList.Get(77568))
	t.Log(skipList.Get(7785614))
}

func TestSkipListPrintMethod(t *testing.T) {
	skipList := skiplist.NewSkipList(77)
	skipList.Put(778, []byte("sda"))
	skipList.Put(7738, []byte("1"))
	skipList.Put(72178, []byte("true"))
	skipList.Put(71378, []byte("114"))
	skipList.Put(72378, []byte("114"))
	skipList.Put(73478, []byte("???"))
	skipList.Put(77568, []byte("&&&"))
	skipList.PrintSkipList()
}

func TestSkipListDeleteMethod(t *testing.T) {
	skipList := skiplist.NewSkipList(7)
	skipList.Put(778, []byte("sda"))
	skipList.Put(7738, []byte("1"))
	skipList.Put(72178, []byte("true"))
	skipList.Put(71378, []byte("114"))
	skipList.Put(72378, []byte("114"))
	skipList.Put(73478, []byte("???"))
	skipList.Put(77568, []byte("&&&"))
	t.Log(skipList.Get(73478))
	skipList.Delete(73478)
	t.Log(skipList.Get(73478))
}

func TestSkipListUpdateMethod(t *testing.T) {
	skipList := skiplist.NewSkipList(7)
	skipList.Put(778, []byte("sda"))
	skipList.Put(7738, []byte("1"))
	skipList.Put(72178, []byte("true"))
	skipList.Put(71378, []byte("114"))
	skipList.Put(72378, []byte("114"))
	skipList.Put(73478, []byte("???"))
	skipList.Put(77568, []byte("&&&"))
	t.Log(skipList.Get(73478))
	skipList.Update(73478, []byte("1919810"))
	t.Log(skipList.Get(73478))
	t.Log(skipList.Get(72378))
	t.Log(skipList.Get(72178))
}

func TestSkipListPutMethod(t *testing.T) {
	// 性能低到离谱的原因很有可能是因为层高过低...
	skipList := skiplist.NewSkipList(77)
	t2 := time.Now().Unix()
	for i := 0; i < 11451419; i += 1 {
		skipList.Put(i, []byte("114514"))
	}

	t.Log(time.Now().Unix() - t2)
	m := make(map[int]interface{})
	t1 := time.Now().Unix()
	for i := 0; i < 11451419; i++ {
		m[i] = 114514
	}
	t.Log(time.Now().Unix() - t1)
}
