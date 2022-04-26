package main

import (
	"Dandelion/skiplist"
)

func main() {
	skipList := skiplist.NewSkipList(5)
	skipList.Insert(1, 114514)
	skipList.Insert(21, 114514)
	skipList.Insert(121, 114514)
	skipList.Insert(1231, 114514)
	skipList.Insert(12231, 114514)
	skipList.Insert(23421, 114514)
	skipList.Insert(32451, 114514)
	skipList.Insert(1322, 114514)
	skipList.Insert(1324, 114514)
	skipList.PrintSkipList()
}
