package main

import (
	"Dandelion/lsm"
	"fmt"
	"log"
	"math/rand"
	"strconv"
)

func main() {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(114) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	for _, i := range rand.Perm(114) {
		if values, ok := l.Get(i); ok {
			fmt.Println(i, string(values))
		}
	}
	fmt.Println(l.Get(114514))
}
