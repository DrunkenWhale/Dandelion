package skiplist

type Node struct {
	key   int
	value interface{}
	right *Node
	down  *Node
}

func NewNode(key int, value interface{}) *Node {
	return &Node{
		key:   key,
		value: value,
	}
}
