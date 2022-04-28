package sstable

import (
	"Dandelion/skiplist"
	"os"
)

const (
	filePrefix = "dandelion_db_storage"
)

var currentProjectPath, _ = os.Getwd()
var currentStoragePath = currentProjectPath + string(os.PathSeparator) + "data"

func StorageData(list *skiplist.SkipList) {
	list.ExportAllElement()
	//disk.WriteDBFile()
}
