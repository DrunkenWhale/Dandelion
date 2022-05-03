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

//func TestStorageOperation(t *testing.T) {
//	f := filter.NewBloomFilter()
//	for i := 0; i < 114514; i++ {
//		f.Put(i)
//	}
//	err := f.freezeBloomFilterDataToFile()
//	if err != nil {
//		log.Fatalln(err)
//	}
//	file, err := f.readBloomFilterDataFromFile()
//	if err != nil {
//		log.Fatalln(err)
//	}
//	t.Log(file)
//	fmt.Println(filter.GeneratorFilterFromFile().Get(114))
//}
