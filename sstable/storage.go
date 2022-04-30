package sstable

import (
	"Dandelion/skiplist"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	storageFilePrefix     = "dandelion_db_storage_data_"
	storageFilePathPrefix = "data" + string(os.PathSeparator)
	level1MaxSize         = 1024 * 1024 * 8
)

var currentProjectPath, _ = os.Getwd()
var currentStoragePath = currentProjectPath + string(os.PathSeparator) + "data" + string(os.PathSeparator)

func FreezeData(list *skiplist.SkipList) error {
	filename := nextDBStorageFileName()
	oldKV, err := ReadDBFile(storageFilePathPrefix + filename)
	if err != nil {
		return err
	}
	newKV := list.ExportAllElement()
	res := KVArrayMerge(oldKV, newKV)
	err = WriteDBFile(storageFilePathPrefix+filename, res)
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
		if !entity.IsDir() && strings.HasPrefix(entity.Name(), storageFilePrefix) {
			res = entity
		}
	}
	if res == nil {
		return storageFilePrefix + strconv.FormatInt(time.Now().Unix(), 10)
	}
	info, err := res.Info()
	if info.Size() > level1MaxSize {
		return storageFilePrefix + strconv.FormatInt(time.Now().Unix(), 10)
	}
	return info.Name()

}

func GetDBFileList() ([]string, error) {
	dir, err := os.ReadDir(currentStoragePath)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for i := len(dir) - 1; i >= 0; i-- {
		res = append(res, dir[i].Name())
	}
	return res, nil
}
