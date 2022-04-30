package sstable

import (
	"Dandelion/skiplist"
	"Dandelion/util"
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	sep               = util.Sep
	defaultBufferSize = 2 * 4096
	closingBound      = defaultBufferSize * 9 / 10
	dataFilePrefix    = "dandelion_db_storage_data_"
	indexFilePrefix   = "dandelion_db_storage_index_"
	filePathPrefix    = "data" + string(os.PathSeparator)
	level1MaxSize     = 1024 * 1024 * 8
)

var (
	currentProjectPath, _ = os.Getwd()
	currentStoragePath    = currentProjectPath + string(os.PathSeparator) + "data" + string(os.PathSeparator)
)

func WriteDBDataFile(filename string, kv []*util.KV) error {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
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
			return err
		}
		if closingBound < buf.Buffered() {
			err = buf.Flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func WriteDBIndexFile(filename string, koffset []*util.KOffset) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(file)

	buf := bufio.NewWriterSize(file, defaultBufferSize)
	for _, entity := range koffset {
		_, err := buf.Write(entity.ToByteArray())
		if err != nil {
			log.Fatalln(err)
			return err
		}
		if closingBound < buf.Buffered() {
			err = buf.Flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReadDBFile(filename string) ([]*util.KV, error) {
	kvArray := make([]*util.KV, 0)
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(file)

	if err != nil {
		return nil, err
	}

	buf := bufio.NewReaderSize(file, defaultBufferSize)
	for {
		keyBytes, err := buf.ReadString(sep)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		valueBytes, err := buf.ReadString(sep)
		if err != nil {
			return nil, err
		}
		key, err := strconv.Atoi(keyBytes[:len(keyBytes)-1])
		if err != nil {
			return nil, err
		}
		kvArray = append(kvArray, util.NewKV(key, []byte(valueBytes[:len(valueBytes)-1])))
	}

	return kvArray, nil
}

func GetDBDataFileList() ([]string, error) {
	dir, err := os.ReadDir(currentStoragePath)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for i := len(dir) - 1; i >= 0; i-- {
		if !dir[i].IsDir() && strings.HasPrefix(dir[i].Name(), dataFilePrefix) {
			res = append(res, dir[i].Name())
		}
	}
	return res, nil
}

func GetDBIndexFileList() ([]string, error) {
	dir, err := os.ReadDir(currentStoragePath)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for i := len(dir) - 1; i >= 0; i-- {
		if !dir[i].IsDir() && strings.HasPrefix(dir[i].Name(), indexFilePrefix) {
			res = append(res, dir[i].Name())
		}
	}
	return res, nil
}

func FreezeDataToFile(list *skiplist.SkipList) error {
	filename := nextDBStorageFileName()
	oldKV, err := ReadDBFile(filePathPrefix + filename)
	if err != nil {
		return err
	}
	newKV := list.ExportAllElement()
	res := KVArrayMerge(oldKV, newKV)
	err = WriteDBDataFile(filePathPrefix+filename, res)
	if err != nil {
		return err
	}
	return nil
}

func nextDBStorageFileName() string {
	dir, err := os.ReadDir(currentStoragePath)
	if err != nil {
		log.Fatalln(err)
		return ""
	}

	var res os.DirEntry = nil

	for _, entity := range dir {
		if !entity.IsDir() && strings.HasPrefix(entity.Name(), dataFilePrefix) {
			res = entity
		}
	}
	if res == nil {
		return dataFilePrefix + strconv.FormatInt(time.Now().Unix(), 10)
	}
	info, err := res.Info()
	if info.Size() > level1MaxSize {
		return dataFilePrefix + strconv.FormatInt(time.Now().Unix(), 10)
	}
	return info.Name()

}
