package skiplist

import (
	"Dandelion/util"
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	head       *Node
	maxLevel   int
	nodeNumber int
	size       int
	memorySize int
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:       NewNode(-1, nil, maxLevel),
		maxLevel:   maxLevel,
		nodeNumber: 0,
	}
}

func (skipList *SkipList) Put(key int, value []byte) {
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
		if cursor.forward[i].key == key {
			skipList.memorySize += len(value) - len(cursor.forward[i].value)
			cursor.forward[i].value = value
			return
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
	skipList.size++
}

func (skipList *SkipList) Get(key int) []byte {
	node := skipList.search(key)
	if node == nil {
		return nil
	} else {
		return node.value
	}
}

func (skipList *SkipList) Update(key int, value []byte) bool {
	node := skipList.search(key)
	if node == nil {
		return false
	} else {
		skipList.memorySize += len(value) - len(node.value)
		node.value = value
		return true
	}
}

func (skipList *SkipList) Delete(key int) {
	cursor := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		if cursor.forward[i] == nil {
			// this cursor is the tail in the same layer nodes
			continue
		}
		for key > cursor.forward[i].key {
			cursor = cursor.forward[i]
			if nil == cursor.forward[i] {
				break
			}
		}

		if nil == cursor.forward[i] {
			continue
		}

		if key == cursor.forward[i].key {
			if i == 0 && cursor.forward[i].forward[i] != nil {
				skipList.memorySize -= len(cursor.forward[i].value)
				skipList.size--
				cursor.forward[i].forward[i].backward = cursor.forward[i]
			}
			cursor.forward[i] = cursor.forward[i].forward[i]
		}
	}
}

func (skipList *SkipList) search(key int) *Node {
	cursor := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		if cursor.forward[i] == nil {
			// this cursor is the tail in the same layer nodes
			continue
		}
		for key > cursor.forward[i].key {
			cursor = cursor.forward[i]
			if nil == cursor.forward[i] {
				break
			}
		}

		if nil == cursor.forward[i] {
			continue
		}

		if key == cursor.forward[i].key {
			return cursor.forward[i]
		}
	}
	return nil
}

func (skipList *SkipList) ExportAllElement() []*util.KV {
	res := make([]*util.KV, 0)
	cursor := skipList.head.forward[0]
	for nil != cursor {
		res = append(res, util.NewKV(cursor.key, cursor.value))
		cursor = cursor.forward[0]
	}
	return res
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
	fmt.Println("\n--------------------------")
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		fmt.Print("*")
		head := start.forward[i]
		for head != nil {
			fmt.Print("->", head.key)
			head = head.forward[i]
		}
		fmt.Println()
	}
	fmt.Println("\n--------------------------")
}

// Size skip list's element size ,exclude pointer
func (skipList SkipList) Size() int {
	return skipList.size
}

// MemorySize skip list's element's value size,exclude key and pointer
func (skipList *SkipList) MemorySize() int {
	return skipList.memorySize
}

func init() {
	rand.Seed(time.Now().Unix())
}
