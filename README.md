# Dandelion

an easy, delicate kv storage engine based on lsm

## use with golang

```go

package main

import (
	"Dandelion/lsm"
	"Dandelion/sstable"
	"log"
	"math/rand"
	"strconv"
)

func main() {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(1145147) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	err := sstable.MergeLevel0File()
	if err != nil {
		log.Fatalln(err)
	}
	values, ok := l.Get(114)
	log.Println(string(values), ok)
	// output 114, true
	err = l.Update(114, []byte("114514"))
	err = l.Delete(114)
	values, ok = l.Get(114)
	//output nil, false
	log.Println(string(values), ok)
}
```