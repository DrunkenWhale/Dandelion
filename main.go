package main

import (
	"Dandelion/sstable"
	"log"
)

func main() {
	table := sstable.NewSSTable()
	for i := 0; i < 114514; i++ {
		err := table.Put(i, []byte("114514"))
		if err != nil {
			log.Fatalln(err)
		}
	}

}
