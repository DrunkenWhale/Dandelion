package test

import (
	"Dandelion/lsm"
	"log"
	"math/rand"
	"strconv"
	"testing"
)

func TestLSMTree(t *testing.T) {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(114) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	for _, i := range rand.Perm(114) {
		if values, ok := l.Get(i); ok {
			t.Log(i, string(values))
		}
	}
	t.Log(l.Get(114514))

}
