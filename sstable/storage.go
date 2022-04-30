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
	indexRangeSize    = 32
)

var (
	currentProjectPath, _ = os.Getwd()
	currentStoragePath    = currentProjectPath + string(os.PathSeparator) + "data" + string(os.PathSeparator)
)

func WriteDBToFile(suffix string, kv []*util.KV) error {

	file, err := os.OpenFile(filePathPrefix+dataFilePrefix+suffix, os.O_WRONLY|os.O_CREATE, 0777)
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

	kIndexes := make([]*util.KIndex, 0)

	start, end := 0, 0

	for index, entity := range kv {
		entityBytes := entity.ToByteArray()
		_, err := buf.Write(entityBytes)
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
		end += len(entityBytes)
		if index%indexRangeSize == 0 {
			kIndexes = append(kIndexes, util.NewKIndex(entity.Key, start, end))
			start = end + 1
		}
	}
	err = writeDBIndexToFile(suffix, kIndexes)
	if err != nil {
		return err

	}
	return nil
}

func writeDBIndexToFile(suffix string, koffset []*util.KIndex) error {
	file, err := os.OpenFile(filePathPrefix+indexFilePrefix+suffix, os.O_WRONLY|os.O_CREATE, 0777)
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

func ReadAllDBDataFromFile(suffix string) ([]*util.KV, error) {
	kvArray := make([]*util.KV, 0)
	file, err := os.OpenFile(filePathPrefix+dataFilePrefix+suffix, os.O_RDONLY|os.O_CREATE, 0777)
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

func readDBIndexFile(suffix string) ([]*util.KIndex, error) {
	kIndexArray := make([]*util.KIndex, 0)
	file, err := os.OpenFile(filePathPrefix+indexFilePrefix+suffix, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(file)

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
		startBytes, err := buf.ReadString(sep)
		if err != nil {
			return nil, err
		}
		endBytes, err := buf.ReadString(sep)
		if err != nil {
			return nil, err
		}
		key, err := strconv.Atoi(keyBytes[:len(keyBytes)-1])
		start, err := strconv.Atoi(keyBytes[:len(startBytes)-1])
		end, err := strconv.Atoi(keyBytes[:len(endBytes)-1])
		if err != nil {
			return nil, err
		}
		kIndexArray = append(kIndexArray, util.NewKIndex(key, start, end))
	}

	return kIndexArray, nil
}

func FreezeDataToFile(list *skiplist.SkipList) error {
	suffixString, err := nextDBFileSuffix()
	if err != nil {
		return err
	}
	oldKV, err := ReadAllDBDataFromFile(suffixString)
	if err != nil {
		return err
	}
	newKV := list.ExportAllElement()
	res := KVArrayMerge(oldKV, newKV)
	err = WriteDBToFile(suffixString, res)
	if err != nil {
		return err
	}
	return nil
}

func nextDBFileSuffix() (string, error) {
	dir, err := os.ReadDir(currentStoragePath)
	if err != nil {
		return "", nil
	}

	var res os.DirEntry = nil

	for _, entity := range dir {
		if !entity.IsDir() && strings.HasPrefix(entity.Name(), dataFilePrefix) {
			res = entity
		}
	}
	timeString := strconv.FormatInt(time.Now().Unix(), 10)
	if res == nil {
		return timeString, nil
	}
	info, err := res.Info()
	if info.Size() > level1MaxSize {
		return timeString, nil
	}
	splitArray := strings.Split(info.Name(), "_")
	fileTimeString := splitArray[len(splitArray)-1]
	return fileTimeString, nil

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
