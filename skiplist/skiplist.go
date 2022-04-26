package skiplist

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	head       *Node
	maxLevel   int
	nodeNumber int
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:       NewNode(-1, nil, maxLevel),
		maxLevel:   maxLevel,
		nodeNumber: 0,
	}
}

func (skipList *SkipList) Insert(key int, value interface{}) {
	level := skipList.randomLevel()
	update := make([]*Node, level)
	cursor := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		if cursor.forward[i] == nil {
			if i < level {
				update[i] = cursor
			}
			continue
		}
		for key > cursor.forward[i].key {
			cursor = cursor.forward[i]
			if nil == cursor.forward[i] {
				break
			}
		}
		if i < level {
			update[i] = cursor
			// add new node in this node tail
		}
	}
	node := NewNode(key, value, level)
	node.backward = update[0]
	for i := 0; i < level; i++ {
		//if update[i].backward == nil {
		//	// head node
		//	update[i].forward[i] = node
		//} else {
		node.forward[i] = update[i].forward[i]
		if update[i].forward[i] != nil {
			// not a tail node
			update[i].forward[i].backward = node
		}
		update[i].forward[i] = node
	}
}

func (skipList *SkipList) Get(key int) interface{} {
	cursor := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		if cursor.forward[i] == nil {
			continue
		}
		for key > cursor.forward[i].key {
			cursor = cursor.forward[i]
			if cursor.forward[i] == nil {
				break
			}
		}
		if key == cursor.key {
			return cursor.value
		}
	}
	return nil
}

const (
	p = 0.5
)

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

func (skipList *SkipList) PrintSkipList() {
	start := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		fmt.Print("*")
		head := start.forward[i]
		for head != nil {
			fmt.Print("->", head.key)
			head = head.forward[i]
		}
		fmt.Println()
	}
}

func init() {
	rand.Seed(time.Now().Unix())
}
