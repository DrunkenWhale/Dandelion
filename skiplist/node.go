package skiplist

type Node struct {
	key     int
	value   interface{}
	next    *Node
	forward *Node
}

func NewNode(key int) *Node {
	return &Node{
		key:     key,
		next:    nil,
		forward: nil,
	}
}
