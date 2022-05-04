package main

import (
	"Dandelion/lsm"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(1145147) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	values, ok := l.Get(114)
	fmt.Println(string(values), ok)
	time.Sleep(time.Second * 1000)
}
