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
	sep               byte = util.Sep
	defaultBufferSize      = 2 * 4096
	closingBound           = defaultBufferSize * 9 / 10
)

func WriteDBFile(filename string, kv []*util.KV) {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
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

	buf := bufio.NewWriterSize(file, defaultBufferSize)
	for _, entity := range kv {
		_, err := buf.Write(entity.ToByteArray())
		if err != nil {
			log.Fatalln(err)
			return
		}
		if closingBound < buf.Buffered() {
			err = buf.Flush()
			if err != nil {
				log.Fatalln(err)
				return
			}
		}
	}

}

func ReadDBFile(filename string) []*util.KV {
	kvArray := make([]*util.KV, 0)
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0777)
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

	if err != nil {
		log.Fatalln(err)
		return nil
	}

	buf := bufio.NewReaderSize(file, defaultBufferSize)
	for {
		keyBytes, err := buf.ReadString(sep)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln(err)
				return nil
			}
		}
		valueBytes, err := buf.ReadString(sep)
		if err != nil {
			log.Fatalln(err)
			return nil
		}
		key, err := strconv.Atoi(keyBytes[:len(keyBytes)-1])
		if err != nil {
			log.Fatalln(err)
			return nil
		}
		kvArray = append(kvArray, util.NewKV(key, []byte(valueBytes[:len(valueBytes)-1])))
	}

	return kvArray
}
