package filter

import "log"

type BitMap struct {
	size int
	core []uint64
}

func NewBitMap(size int) *BitMap {
	return &BitMap{
		size: size,
		core: make([]uint64, 1+(size/64)),
	}
}

func (bitmap *BitMap) Put(key int) {
	if key > bitmap.size {
		log.Fatalln("key bigger than bitmap max size")
	}
	arrayIndex := key / 64
	bitIndex := key % 64
	num := bitmap.core[arrayIndex]
	bitmap.core[arrayIndex] = num | (1 << bitIndex)
}

func (bitmap *BitMap) Get(key int) bool {
	if key > bitmap.size {
		log.Fatalln("key bigger than bitmap max size")
	}
	arrayIndex := key / 64
	bitIndex := key % 64
	num := bitmap.core[arrayIndex]
	return 0 != (num & (1 << bitIndex))

}
