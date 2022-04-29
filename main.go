package main

import (
	"Dandelion/sstable"
	"log"
	"math/rand"
	"strconv"
)

func main() {
	table := sstable.NewSSTable()
	for i := 0; i < 1145147; i++ {
		err := table.Put(rand.Int(), []byte(strconv.Itoa(rand.Int())))
		if err != nil {
			log.Fatalln(err)
			return
		}
	}
}
