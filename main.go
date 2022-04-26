package main

import (
	"Dandelion/skiplist"
	"expvar"
	"fmt"
	"time"
)

func main() {
	skipList := skiplist.NewSkipList(5)
	t := time.Now().Unix()
	for i := 0; i < 114514; i++ {
		skipList.Insert(i, 114514)
	}
	fmt.Println(time.Now().Unix() - t)
	a := expvar.NewMap("name")
	t1 := time.Now().Unix()
	for i := 0; i < 114514; i++ {
		a.Add("114514", 114514)
	}
	for i := 0; i < 114514; i++ {
		a.Add("114514", 114514)
	}
	fmt.Println(time.Now().Unix() - t1)
	//skipList.PrintSkipList()
}
