package main

import (
	"Dandelion/skiplist"
	"fmt"
)

func main() {
	skipList := skiplist.NewSkipList(32)
	skipList.Put(778, "sda")
	skipList.Put(7738, 1)
	skipList.Put(72178, true)
	skipList.Put(71378, 114)
	skipList.Put(72378, "114")
	skipList.PrintSkipList()
	fmt.Println("--------------------------")
	fmt.Println(skipList.Get(72378))

}
