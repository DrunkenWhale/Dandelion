package main

import "Dandelion/skiplist"

func main() {
	skipList := skiplist.NewSkipList(32)
	skipList.Insert(778, "sda")
	skipList.Insert(7738, 1)
	skipList.Insert(72178, true)
	skipList.Insert(71378, 114)
	skipList.Insert(72378, "114")
	skipList.Insert(73178, true)
	skipList.Insert(71328, 114)
	skipList.Insert(72378, "114")
	skipList.Insert(73478, "???")
	skipList.Insert(77568, "&&&")
	skipList.PrintSkipList()
}
