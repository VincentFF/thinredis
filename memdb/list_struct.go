package memdb

import "bytes"

// List implements a double linked list for redis list
type List struct {
    Head *ListNode
    Tail *ListNode
    Len  int
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
    return &List{Head: head, Tail: tail, Len: 0}
}

func (l *List) Index(index int) *ListNode {
    var node *ListNode
    if index < 0 {
        if -index > l.Len {
            return nil
        }
        node = l.Tail.Prev
        for i := -1; i > index; i-- {
            node = node.Prev
        }
    } else {
        if index >= l.Len {
            return nil
        }
        node = l.Head.Next
        for i := 0; i < index; i++ {
            node = node.Next
        }
    }
    return node
}

func (l *List) Pos(val []byte) int {
    pos := 0
    for now := l.Head.Next; now != l.Tail; now = now.Next {
        if bytes.Equal(now.Val, val) {
            return pos
        }
        pos++
    }
    return -1
}

func (l *List) LPush(val []byte) {
    node := &ListNode{Prev: l.Head, Next: l.Head.Next, Val: val}
    l.Head.Next = node
    node.Next.Prev = node
    l.Len++
}

func (l *List) RPush(val []byte) {
    node := &ListNode{Prev: l.Tail.Prev, Next: l.Tail, Val: val}
    l.Tail.Prev = node
    node.Prev.Next = node
    l.Len++
}

func (l *List) LPop() *ListNode {
    if l.Len == 0 {
        return nil
    }
    node := l.Head.Next
    l.Head.Next = node.Next
    node.Next.Prev = l.Head
    node.Prev = nil
    node.Next = nil
    l.Len--
    return node
}

func (l *List) RPop() *ListNode {
    if l.Len == 0 {
        return nil
    }
    node := l.Tail.Prev
    l.Tail.Prev = node.Prev
    node.Prev.Next = l.Tail
    node.Prev = nil
    node.Next = nil
    l.Len--
    return node
}

func (l *List) Set(index int, val []byte) bool {
    if index < 0 {
        if -index > l.Len {
            return false
        }
        node := l.Tail
        for node != l.Head && index < 0 {
            node = node.Prev
            index++
        }
        node.Val = val
    } else {
        if index >= l.Len {
            return false
        }
        node := l.Head
        for node != l.Tail && index >= 0 {
            node = node.Next
            index--
        }
        node.Val = val
    }
    return true
}

func (l *List) Range(start, end int) [][]byte {
    if start < 0 {
        start = l.Len + start
    }
    if end < 0 {
        end = l.Len + end
    }
    if start > end || start >= l.Len || end < 0 {
        return nil
    }

    if start < 0 {
        start = 0
    }

    if end >= l.Len {
        end = l.Len - 1
    }

    res := make([][]byte, 0, end-start+1)
    node := l.Head
    for i := 0; i <= end; i++ {
        node = node.Next
        if i >= start {
            res = append(res, node.Val)
        }
    }
    return res
}

func (l *List) InsertBefore(val []byte, tar []byte) int {
    pos := 0
    ok := false
    for now := l.Head.Next; now != l.Tail; now = now.Next {
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
    for now := l.Head.Next; now != l.Tail; now = now.Next {
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

// RemoveElement remove count number elements with Val=val from list, if count is 0, remove all elements.
// return the number of elements removed.
// if count>0, remove from head to tail, otherwise remove from tail to head
func (l *List) RemoveElement(val []byte, count int) int {
    if l.Len == 0 {
        return 0
    }

    if count == 0 {
        count = l.Len - 1
    }

    removed := 0
    var tem *ListNode
    if count >= 0 {
        for now := l.Head.Next; now != l.Tail && removed < count; {
            if bytes.Equal(now.Val, val) {
                tem = now.Next
                now.Prev.Next = now.Next
                now.Next.Prev = now.Prev
                now.Prev = nil
                now.Next = nil
                removed++
                now = tem
            } else {
                now = now.Next
            }
        }
    } else {
        for now := l.Tail.Prev; now != l.Head && removed < -count; {
            if bytes.Equal(now.Val, val) {
                tem = now.Prev
                now.Prev.Next = now.Next
                now.Next.Prev = now.Prev
                now.Prev = nil
                now.Next = nil
                removed++
                now = tem
            } else {
                now = now.Prev
            }
        }
    }
    return removed
}

func (l *List) Trim(start, end int) {
    if l.Len == 0 {
        return
    }
    if start < 0 {
        start = l.Len + start
    }
    if end < 0 {
        end = l.Len + end
    }
    if start > end || start >= l.Len || end < 0 {
        l.Clear()
        return
    }

    if start < 0 {
        start = 0
    }

    if end >= l.Len {
        end = l.Len - 1
    }

    var startNode, endNode *ListNode
    pos := 0
    for now := l.Head.Next; now != l.Tail; now = now.Next {
        if pos == start {
            startNode = now
        }
        if pos == end {
            endNode = now
            break
        }
        pos++
    }

    //	remove discarded nodes for gc
    l.Head.Next.Prev = nil
    l.Tail.Prev.Next = nil
    if startNode.Prev != nil {
        startNode.Prev.Next = nil
    }
    if endNode.Next != nil {
        endNode.Next.Prev = nil
    }

    //	link trimmed nodes to head and tail
    l.Head.Next = startNode
    startNode.Prev = l.Head
    l.Tail.Prev = endNode
    endNode.Next = l.Tail
    l.Len = end - start + 1
}

func (l *List) Clear() {
    if l.Len == 0 {
        return
    }

    fist := l.Head.Next
    last := l.Tail.Prev
    l.Head.Next = l.Tail
    l.Tail.Prev = l.Head
    l.Len = 0

    //	 gc will remove the list
    fist.Prev = nil
    last.Next = nil
}
