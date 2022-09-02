package resp

import (
    "strconv"
)

var (
    CRLF = "\r\n"
)

type RedisData interface {
    ToBytes() []byte  // return resp transfer format data
    ByteData() []byte // return byte data
}

type StringData struct {
    data string
}

type BulkData struct {
    data []byte
}

type IntData struct {
    data int64
}

type ErrorData struct {
    data string
}

// ArrayData not implement ByteData()
type ArrayData struct {
    data []RedisData
}

type PlainData struct {
    data string
}

func MakeBulkData(data []byte) *BulkData {
    return &BulkData{
        data: data,
    }
}

//func MakeNullBulkData() *BulkData {
//	return &BulkData{
//		data: []byte{},
//	}
//}

func (r *BulkData) ToBytes() []byte {
    if r.data == nil {
        return []byte("$-1\r\n")
    }
    return []byte("$" + strconv.Itoa(len(r.data)) + CRLF + string(r.data) + CRLF)
}

func (r *BulkData) Data() []byte {
    return r.data
}

func (r *BulkData) ByteData() []byte {
    return r.data
}

func MakeStringData(data string) *StringData {
    return &StringData{
        data: data,
    }
}

func (r *StringData) ToBytes() []byte {
    return []byte("+" + r.data + CRLF)
}

func (r *StringData) Data() string {
    return r.data
}
func (r *StringData) ByteData() []byte {
    return []byte(r.data)
}

func MakeIntData(data int64) *IntData {
    return &IntData{
        data: data,
    }
}

func (r *IntData) ToBytes() []byte {
    return []byte(":" + strconv.FormatInt(r.data, 10) + CRLF)
}

func (r *IntData) Data() int64 {
    return r.data
}

func (r *IntData) ByteData() []byte {
    return []byte(strconv.FormatInt(r.data, 10))
}

func MakeErrorData(data string) *ErrorData {
    return &ErrorData{
        data: data,
    }
}

func (r *ErrorData) ToBytes() []byte {
    return []byte("-" + r.data + CRLF)
}

func (r *ErrorData) Error() string {
    return r.data
}

func (r *ErrorData) ByteData() []byte {
    return []byte(r.data)
}

func MakeArrayData(data []RedisData) *ArrayData {
    return &ArrayData{
        data: data,
    }
}

func (r *ArrayData) ToBytes() []byte {
    if r.data == nil {
        return []byte("*-1\r\n")
    }

    res := []byte("*" + strconv.Itoa(len(r.data)) + CRLF)
    for _, v := range r.data {
        res = append(res, v.ToBytes()...)
    }
    return res
}
func (r *ArrayData) Data() []RedisData {
    return r.data
}

func (r *ArrayData) TOCommand() [][]byte {
    res := make([][]byte, 0)
    for _, v := range r.data {
        res = append(res, v.ByteData())
    }
    return res
}

// ByteData is discarded. Use ToCommand() instead.
func (r *ArrayData) ByteData() []byte {
    res := make([]byte, 0)
    for _, v := range r.data {
        res = append(res, v.ByteData()...)
    }
    return res
}

func MakePlainData(data string) *PlainData {
    return &PlainData{
        data: data,
    }
}
func (r *PlainData) ToBytes() []byte {
    return []byte(r.data + CRLF)
}
func (r *PlainData) Data() string {
    return r.data
}

func (r *PlainData) ByteData() []byte {
    return []byte(r.data)
}
