package skiplist

import (
	"math/rand"
)

type SkipList struct {
	head       *Node
	maxLevel   int
	nodeNumber int
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:       NewNode(-1, nil),
		maxLevel:   maxLevel,
		nodeNumber: 0,
	}
}

func (skipList *SkipList) Insert(key int, value interface{}) {
	level := skipList.randomLevel()
	update := make([]*Node, level)
	cursor := skipList.head
	for i := skipList.maxLevel - 1; i >= 0; i-- {
		if cursor.right == nil {
			if i < level {
				update[i] = cursor
			}
			continue
		}
		for key > cursor.right.key {
			cursor = cursor.right
			if nil == cursor.right {
				break
			}
		}
		if i < level {
			update[i] = cursor
			// add new node in this node tail
		}
		cursor = cursor.down
	}
	node := NewNode(key, value)
	for i := 0; i < level; i++ {
		//if update[i].backward == nil {
		//	// head node
		//	update[i].forward[i] = node
		//} else {
		node.right = update[i].right
		update[i].right = node
	}
	node.down = NewNode

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

func (skipList *SkipList) PrintSkipList() {
	//start := skipList.head
	//for i := skipList.maxLevel - 1; i >= 0; i-- {
	//	fmt.Print("*")
	//	head := start.forward[i]
	//	for head != nil {
	//		fmt.Print("->", head.key)
	//		head = head.forward[i]
	//	}
	//	fmt.Println()
	//}
}
