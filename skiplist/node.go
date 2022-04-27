package skiplist

type Node struct {
	key      int
	value    []byte
	backward *Node
	forward  []*Node
}

func NewNode(key int, value []byte, level int) *Node {
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level),
	}
}
