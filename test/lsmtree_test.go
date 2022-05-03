package test

import (
	"Dandelion/lsm"
	"Dandelion/sstable"
	"log"
	"math/rand"
	"strconv"
	"testing"
)

func TestLSMTree1(t *testing.T) {
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

func TestLSMTree2(t *testing.T) {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(1145141) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	log.Println(l.Update(114514, []byte("test succeed")))
	log.Println(l.Get(114514))
}

func TestLSMTree3(t *testing.T) {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(1145147) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	err := sstable.MergeLevel0File()
	if err != nil {
		log.Fatalln(err)
	}
	values, ok := l.Get(114)
	t.Log(string(values), ok)
}
