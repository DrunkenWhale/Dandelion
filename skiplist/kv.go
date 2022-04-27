package skiplist

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
