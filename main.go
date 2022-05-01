package main

import (
	"Dandelion/sstable"
	"fmt"
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
	err := sstable.SearchKVFromFile(37)
	if err != nil {
		log.Fatalln(err)
	}

	collection, _ := sstable.readRangeDBDataFromFile("1651397189", 321, 640)
	for _, i2 := range collection {
		fmt.Println(i2.Key, "==>", string(i2.Value))
	}

}
