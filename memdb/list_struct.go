package memdb

import "bytes"

// List implements a double linked list for redis list
type List struct {
    head *ListNode
    tail *ListNode
    len  int
}

type ListNode struct {
    Prev *ListNode
    Next *ListNode
    Val  []byte
}

func NewList() *List {
    head := &ListNode{}
    tail := &ListNode{}
    head.Next = tail
    tail.Prev = head
    return &List{head: head, tail: tail, len: 0}
}

func (l *List) Len() int {
    return l.len
}

func (l *List) Index(index int) *ListNode {
    if index < 0 {
        index = l.len + index
    }
    if index < 0 || index >= l.len {
        return nil
    }
    node := l.head.Next
    for i := 0; i < index; i++ {
        node = node.Next
    }
    return node
}

func (l *List) LPush(val []byte) {
    node := &ListNode{Prev: l.head, Next: l.head.Next, Val: val}
    l.head.Next = node
    node.Next.Prev = node
    l.len++
}

func (l *List) RPush(val []byte) {
    node := &ListNode{Prev: l.tail.Prev, Next: l.tail, Val: val}
    l.tail.Prev = node
    node.Prev.Next = node
    l.len++
}

func (l *List) LPop() *ListNode {
    if l.len == 0 {
        return nil
    }
    node := l.head.Next
    l.head.Next = node.Next
    node.Next.Prev = l.head
    node.Prev = nil
    node.Next = nil
    l.len--
    return node
}

func (l *List) RPop() *ListNode {
    if l.len == 0 {
        return nil
    }
    node := l.tail.Prev
    l.tail.Prev = node.Prev
    node.Prev.Next = l.tail
    node.Prev = nil
    node.Next = nil
    l.len--
    return node
}

func (l *List) InsertBefore(val []byte, tar []byte) int {
    pos := 0
    ok := false
    for now := l.head.Next; now != l.tail; now = now.Next {
        if bytes.Equal(now.Val, tar) {
            ok = true
            node := &ListNode{Prev: now.Prev, Next: now, Val: val}
            now.Prev = node
            node.Prev.Next = node
            break
        }
        pos++
    }
    if ok {
        return pos
    }
    return -1
}

func (l *List) InsertAfter(val []byte, tar []byte) int {
    pos := 0
    ok := false
    for now := l.head.Next; now != l.tail; now = now.Next {
        if bytes.Equal(now.Val, tar) {
            ok = true
            node := &ListNode{Prev: now, Next: now.Next, Val: val}
            now.Next = node
            node.Next.Prev = node
            break
        }
        pos++
    }
    if ok {
        return pos + 1
    }
    return -1
}
