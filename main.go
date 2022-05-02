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
	//values, ok, err := sstable.searchKVFromFile(114513)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//if ok {
	//	fmt.Println(string(values))
	//} else {
	//	log.Println("key unexist")
	//}
}
