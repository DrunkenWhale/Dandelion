package skiplist

type Node struct {
	key      int
	value    interface{}
	backward *Node
	forward  []*Node
}

func NewNode(key int, value interface{}, level int) *Node {
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level),
	}
}
