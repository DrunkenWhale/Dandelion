package main

import (
	"Dandelion/sstable"
	"fmt"
	"log"
	"sort"
)

func main() {
	list, err := sstable.GetDBFileList()
	if err != nil {
		log.Fatalln(err)
		return
	}
	fmt.Println((sort.StringSlice(list)))
	//table := sstable.NewSSTable()
	//for i := 0; i < 1145147; i++ {
	//	err := table.Put(rand.Int(), []byte(strconv.Itoa(rand.Int())))
	//	if err != nil {
	//		log.Fatalln(err)
	//		return
	//	}
	//}
}
