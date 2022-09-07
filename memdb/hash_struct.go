package memdb

import "strconv"

type Hash struct {
	table map[string][]byte
}

func NewHash() *Hash {
	return &Hash{make(map[string][]byte)}
}

func (h *Hash) Set(key string, value []byte) {
	h.table[key] = value
}

func (h *Hash) Get(key string) []byte {
	return h.table[key]
}

func (h *Hash) Del(key string) int {
	if h.Exist(key) {
		delete(h.table, key)
		return 1
	}
	return 0
}

func (h *Hash) Len() int {
	return len(h.table)
}

func (h *Hash) Keys() []string {
	keys := make([]string, 0, len(h.table))
	for key := range h.table {
		keys = append(keys, key)
	}
	return keys
}

func (h *Hash) Values() [][]byte {
	values := make([][]byte, 0, len(h.table))
	for _, value := range h.table {
		values = append(values, value)
	}
	return values
}

func (h *Hash) Clear() {
	h.table = make(map[string][]byte)
}

func (h *Hash) IsEmpty() bool {
	return len(h.table) == 0
}

func (h *Hash) Exist(key string) bool {
	_, ok := h.table[key]
	return ok
}

func (h *Hash) StrLen(key string) int {
	return len(h.table[key])
}

func (h *Hash) Random(count int) []string {
	res := make([]string, 0)
	if count == 0 || h.Len() == 0 {
		return res
	} else if count > 0 {
		if count > h.Len() {
			count = h.Len()
		}
		for key := range h.table {
			res = append(res, key)
			if len(res) == count {
				break
			}
		}
	} else {
		for {
			for key := range h.table {
				res = append(res, key)
				if len(res) == -count {
					return res
				}
			}

		}
	}
	return res
}

func (h *Hash) RandomWithValue(count int) [][]byte {
	res := make([][]byte, 0)
	if count == 0 || h.Len() == 0 {
		return res
	} else if count > 0 {
		if count > h.Len() {
			count = h.Len()
		}
		count *= 2
		for key, val := range h.table {
			res = append(res, []byte(key), val)
			if len(res) == count {
				break
			}
		}
	} else {
		count *= 2
		for {
			for key, val := range h.table {
				res = append(res, []byte(key), val)
				if len(res) == -count {
					return res
				}
			}

		}
	}
	return res
}

func (h *Hash) Table() map[string][]byte {
	return h.table
}

func (h *Hash) IncrBy(key string, incr int) (int, bool) {
	tem := h.Get(key)
	if len(tem) == 0 {
		h.Set(key, []byte(strconv.Itoa(incr)))
		return incr, true
	} else {
		value, err := strconv.Atoi(string(tem))
		if err != nil {
			return 0, false
		}
		value += incr
		h.Set(key, []byte(strconv.Itoa(value)))
		return value, true
	}
}

func (h *Hash) IncrByFloat(key string, incr float64) (float64, bool) {
	tem := h.Get(key)
	if len(tem) == 0 {
		h.Set(key, []byte(strconv.FormatFloat(incr, 'f', -1, 64)))
		return incr, true
	} else {
		value, err := strconv.ParseFloat(string(tem), 64)
		if err != nil {
			return 0, false
		}
		value += incr
		h.Set(key, []byte(strconv.FormatFloat(value, 'f', -1, 64)))
		return value, true
	}
}
