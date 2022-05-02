package test

import (
	"Dandelion/filter"
	"testing"
)

func TestBitMap(t *testing.T) {
	c := filter.NewBitMap(114514)
	for i := 0; i <= 114; i++ {
		c.Put(i * -7)
	}
	for i := 0; i <= 114514; i++ {
		if c.Get(i) {
			t.Log(i)
		}
	}
}
