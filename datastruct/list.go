package dataStruct

type listNode struct {
	pre *listNode
	nex *listNode
	val any
}

type List struct {
	head *listNode
	tail *listNode
	size int
}

func CreateList() List {
	list := List{}
	list.head, list.tail = new(listNode), new(listNode)
	list.head.nex, list.tail.pre = list.tail, list.head
	return list
}

func (l *List) Size() int {
	return l.size
}

func (l *List) InsertBefore(node, insertPos *listNode) {
	node.nex, node.pre = insertPos, insertPos.pre
	insertPos.pre.nex, insertPos.pre = node, node
	l.size++
}

func (l *List) InsertAfter(node, insertPos *listNode) {
	node.pre, node.nex = insertPos, insertPos.nex
	insertPos.nex.pre, insertPos.nex = node, node
	l.size++
}

func (l *List) Head() *listNode {
	return l.head
}

func (l *List) Tail() *listNode {
	return l.tail
}
