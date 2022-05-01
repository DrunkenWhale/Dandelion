package util

import (
	"bytes"
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
func (k *KIndex) ToByteArray() []byte {
	buf := new(bytes.Buffer)
	buf.Write([]byte(strconv.Itoa(k.key)))
	buf.WriteByte(Sep)
	buf.Write([]byte(strconv.Itoa(k.start)))
	buf.WriteByte(Sep)
	buf.Write([]byte(strconv.Itoa(k.end)))
	buf.WriteByte(Sep)
	return buf.Bytes()
}

func (k *KIndex) GetKey() int {
	return k.key
}
func (k *KIndex) GetStart() int {
	return k.start
}
func (k *KIndex) GetEnd() int {
	return k.end
}
