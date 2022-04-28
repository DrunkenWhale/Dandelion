package disk

import (
	"Dandelion/util"
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	sep byte = 3
)

func WriteDBFile(filename string, kv []*util.KV) {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0)
	if err != nil {
		log.Fatalln(err)
		return
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(file)

	for _, entity := range kv {
		_, err := file.Write(entity.ToByteArray())
		if err != nil {
			log.Fatalln(err)
			return
		}
	}
}

func ReadDBFile(filename string) []*util.KV {
	kvArray := make([]*util.KV, 0)
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
		keyBytes, err := buf.ReadSlice(sep)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln(err)
				return nil
			}
		}
		valueBytes, err := buf.ReadSlice(sep)
		if err != nil {
			log.Fatalln(err)
			return nil
		}
		key, err := strconv.Atoi(string(keyBytes[:len(keyBytes)-1]))
		if err != nil {
			log.Fatalln(err)
			return nil
		}
		kvArray = append(kvArray, util.NewKV(key, valueBytes))
	}
	return kvArray
}
