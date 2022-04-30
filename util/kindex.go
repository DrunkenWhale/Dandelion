package util

type KOffset struct {
	key    int
	offset int
}

func NewKOffset(key int, offset int) *KOffset {
	return &KOffset{
		key:    key,
		offset: offset,
	}
}
func (k KOffset) ToByteArray() []byte {
	
}
