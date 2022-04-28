package util

import (
	"bytes"
	"strconv"
)

type KV struct {
	Key   int
	Value []byte
}

func NewKV(key int, value []byte) *KV {
	return &KV{
		Key:   key,
		Value: value,
	}
}

const sep = 3

func (kv *KV) ToByteArray() []byte {
	buf := new(bytes.Buffer)
	buf.WriteString(strconv.Itoa(kv.Key))
	buf.WriteByte(sep)
	buf.Write(kv.Value)
	buf.WriteByte(sep)
	return buf.Bytes()
}
