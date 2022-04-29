package sstable

import (
	"Dandelion/skiplist"
	disk2 "Dandelion/sstable/disk"
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

func StorageData(list *skiplist.SkipList) {
	filename := nextDBStorageFileName()
	oldKV := disk2.ReadDBFile(storageFilePathPrefix + filename)
	newKV := list.ExportAllElement()
	res := disk2.KVArrayMerge(oldKV, newKV)
	disk2.WriteDBFile(storageFilePathPrefix+filename, res)
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
