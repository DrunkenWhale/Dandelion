package util

import (
	"bytes"
	"fmt"
	"strconv"
)

type KIndex struct {
	key   int
	start int
	end   int
}

func NewKIndex(key int, start int, end int) *KIndex {
	return &KIndex{
		key:   key,
		start: start,
		end:   end,
	}
}
func (k KIndex) ToByteArray() []byte {
	buf := new(bytes.Buffer)
	buf.Write([]byte(strconv.Itoa(k.key)))
	buf.WriteByte(Sep)
	buf.Write([]byte(strconv.Itoa(k.start)))
	buf.WriteByte(Sep)
	buf.Write([]byte(strconv.Itoa(k.end)))
	buf.WriteByte(Sep)
	fmt.Println(k)
	return buf.Bytes()
}
