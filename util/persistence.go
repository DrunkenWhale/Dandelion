package util

import (
	"Dandelion/skiplist"
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	sep byte = 3
)

func WriteDBFile(list *skiplist.SkipList) {

	for _, kv := range list.ExportAllElement() {
		fmt.Println(kv.Key, "==>", string(kv.Value))
	}

}

func ReadDBFile(filename string) *skiplist.SkipList {
	skipList := skiplist.NewSkipList(17)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
		return nil
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
			return
		}
	}(file)

	buf := bufio.NewReader(file)
	for {
		keyBytes, err := buf.ReadSlice(sep - 1)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln(err)
				return nil
			}
		}
		valueBytes, err := buf.ReadSlice(sep - 1)
		if err != nil {
			log.Fatalln(err)
			return nil
		}

		key, err := strconv.Atoi(string(keyBytes[:len(keyBytes)-1]))
		if err != nil {
			log.Fatalln(err)
			return nil
		}
		skipList.Put(key, valueBytes)
	}
	return skipList
}
