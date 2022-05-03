package main

import (
	"Dandelion/lsm"
	"fmt"
)

func main() {
	l := lsm.NewLSM()
	//for _, i := range rand.Perm(114514) {
	//	err := l.Put(i, []byte(strconv.Itoa(i)))
	//	if err != nil {
	//		log.Fatalln(err)
	//	}
	//}
	//err := sstable.MergeLevel0File()
	//if err != nil {
	//	log.Fatalln(err)
	//}
	fmt.Println(l.Get(114))
}
