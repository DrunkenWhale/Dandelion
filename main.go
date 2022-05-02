package main

import (
	"Dandelion/filter"
	"fmt"
	"math/rand"
	"sort"
)

func main() {
	//a := crc32.ChecksumIEEE([]byte(strconv.FormatUint(11214514, 10)))
	//fmt.Println(a)
	//a = adler32.Checksum([]byte(strconv.FormatUint(11214514, 10)))
	//fmt.Println(a)
	//c := fnv.New32()
	//_, err := c.Write([]byte(strconv.FormatUint(11214514, 10)))
	//if err != nil {
	//	return
	//}
	//a = c.Sum32()
	//fmt.Println(a)
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
	fmt.Println(float64(count*1.0) / 114514)
}
