package test

import (
	"Dandelion/filter"
	"math/rand"
	"sort"
	"testing"
)

func TestBloomFilter(t *testing.T) {
	p := rand.Perm(114514)
	sort.Ints(p)
	f := filter.NewBloomFilter()
	for _, t := range p {
		f.Put(t)
	}
	count := 0
	for i := 0; i < len(p); i++ {
		if f.Get(114514 + i) {
			count++
		}
	}
	//wrong judge rate
	t.Log(float64(count*1.0) / 114514)
}
