package main

import (
	"Dandelion/skiplist"
	"Dandelion/util"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	skipList := skiplist.NewSkipList(17)
	skipList.Put(778, []byte("sda"))
	skipList.Put(7738, []byte("1"))
	skipList.Put(72178, []byte("true"))
	skipList.Put(71378, []byte("114"))
	skipList.Put(72378, []byte("114"))
	skipList.Put(73478, []byte("???"))
	skipList.Put(77568, []byte("&&&"))
	util.WriteDBFile(skipList)
	f, err := os.OpenFile("test.txt", os.O_WRONLY|os.O_CREATE, 0666)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)
	if err != nil {
		log.Fatalln(err)
	}
	var ch byte = 2
	_, err = f.Write(append([]byte(strconv.Itoa(1)), ch))
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%c", ch)
	_, err = f.Write(append([]byte("114"), ch))
	if err != nil {
		log.Print(err)
	}
	l := util.ReadDBFile("test.txt")
	l.PrintSkipList()
}
