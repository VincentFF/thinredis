package memdb

type Hash struct {
    table map[string][]byte
}

func NewHash() *Hash {
    return &Hash{make(map[string][]byte)}
}
