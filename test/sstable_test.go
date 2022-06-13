package test

import (
	"Dandelion/sstable"
	"log"
	"testing"
)

func TestSSTable(t *testing.T) {
	table := sstable.NewSSTable()

	for i := 0; i < 11451; i++ {
		err := table.Put(i, []byte("114514"))
		if err != nil {
			log.Fatalln(err)
		}
	}
	s, _ := table.Get(114)
	t.Log(string(s))
	err := table.Delete(114)
	if err != nil {
		log.Fatalln(err)
	}
	err = table.Update(114, []byte("test"))
	if err != nil {
		log.Fatalln(err)
	}
	s, ok := table.Get(114)
	t.Log(string(s), ok)
}
