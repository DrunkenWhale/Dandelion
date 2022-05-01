package main

import (
	"Dandelion/sstable"
	"log"
)

func main() {
	table := sstable.NewSSTable()

	for i := 0; i < 70007; i++ {
		err := table.Put(i, []byte("114514"))
		if err != nil {
			log.Fatalln(err)
		}
	}
	err := sstable.SearchKVFromFile(45824)
	if err != nil {
		log.Fatalln(err)
	}
}
