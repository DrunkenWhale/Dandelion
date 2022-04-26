package skiplist

import "math/rand"

type SkipList struct {
	head       *Node
	maxLevel   int
	nodeNumber int
}

func (skipList *SkipList) NewSkipList() {

}

func (skipList *SkipList) Insert(key int, value interface{}) {
	level := skipList.randomLevel()
	cursor := skipList.head
	for i := 0; i < skipList.maxLevel; i++ {
		for cursor.key > cursor.next.key {
			cursor = cursor.next
		}
		cursor = cursor.forward
	}

}

const p = 0.5

// have p/2 probability return 1
// have p/4 probability return 2
// have p/8 probability return 3
// and so on
func (skipList *SkipList) randomLevel() int {
	level := 1
	for rand.Float64() < p && level < skipList.maxLevel {
		level++
	}
	return level
}
