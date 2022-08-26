package resp

import (
    "bufio"
    "bytes"
    "fmt"
    "io"
    "testing"

    "github.com/VincentFF/simpleredis/config"
    "github.com/VincentFF/simpleredis/logger"
)

func init() {
    cfg := &config.Config{
        LogLevel: "debug",
        LogDir:   "/tmp",
    }
    err := logger.SetUp(cfg)
    if err != nil {
        fmt.Println("logger setup error")
    }
    logger.Disable()
}

func TestParseBulkHeader(t *testing.T) {
    state := new(readState)
    headers := make([][]byte, 5)
    for i := -2; i < 3; i++ {
        s := fmt.Sprintf("$%d\r\n", i)
        headers[i+2] = []byte(s)
    }
    for i := -2; i < 3; i++ {
        header := headers[i+2]
        err := parseBulkHeader(header, state)
        if i < -1 {
            if err == nil || state.bulkLen != 0 || state.multiLine {
                t.Error(fmt.Sprintf("parseBulkHeader(%v) == %v, %v, %v | expect:  error, 0, false", header, err, state.bulkLen, state.multiLine))
            }
        } else {
            if state.bulkLen != int64(i) || !state.multiLine || err != nil {
                t.Error(fmt.Sprintf("parseBulkHeader(%v) == %v, %v, %v | expect:  nil, %d, true", header, err, state.bulkLen, state.multiLine, i))
            }
        }
    }
}

func TestParseArrayHeader(t *testing.T) {
    state := new(readState)
    headers := make([][]byte, 5)
    for i := -2; i < 3; i++ {
        s := fmt.Sprintf("*%d\r\n", i)
        headers[i+2] = []byte(s)
    }
    for i := -2; i < 3; i++ {
        header := headers[i+2]
        err := parseArrayHeader(header, state)
        if i < -1 {
            if err == nil || state.arrayLen != 0 || state.inArray {
                t.Error(fmt.Sprintf("parseArrayHeader(%v) == %v, %v, %v | expect:  error, 0, false", header, err, state.arrayLen, state.inArray))
            }
        } else {
            if err != nil || state.arrayLen != i || !state.inArray {
                t.Error(fmt.Sprintf("parseArrayHeader(%v) == %v, %v, %v | expect:  nil, %d, true", header, err, state.arrayLen, state.inArray, i))
            }
        }
    }
}

func TestParseMultiLine(t *testing.T) {
    msg := []byte("abc\r\n")
    bulk := BulkData{[]byte("abc")}
    res, err := parseMultiLine(msg)
    if err != nil {
        t.Error("parseMultiLine() error != nil | except: nil")
    }
    if !bytes.Equal(bulk.data, res.(*BulkData).data) {
        t.Error("parseMultiLine() res.data != test.data")
    }
    if !bytes.Equal((&bulk).ToBytes(), res.ToBytes()) {
        t.Error("parseMultiLine() res.ToBytes() != test.ToBytes()")
    }
}

func TestParseSingleLine(t *testing.T) {
    sMsg := []byte("+abc\r\n")
    eMsg := []byte("-err\r\n")
    iMsg := []byte(":-20\r\n")
    iMsg2 := []byte(":2a\r\n")
    sRes, err := parseSingleLine(sMsg)
    if !bytes.Equal(sRes.ToBytes(), sMsg) || sRes.(*StringData).data != "abc" || err != nil {
        t.Error(fmt.Sprintf("parseSingleLine( %s ) error", string(sMsg)))
    }
    eRes, err := parseSingleLine(eMsg)
    if !bytes.Equal(eRes.ToBytes(), eMsg) || eRes.(*ErrorData).data != "err" || err != nil {
        t.Error(fmt.Sprintf("parseSingleLine(%s) error.", string(eMsg)))
        t.Error(fmt.Sprintf("get %v, %v, %v | expect:  nil, %v, err", err, eRes.ToBytes(), eRes.(*ErrorData).data, eMsg))
    }
    iRes, err := parseSingleLine(iMsg)
    if !bytes.Equal(iRes.ToBytes(), iMsg) || iRes.(*IntData).data != -20 || err != nil {
        t.Error(fmt.Sprintf("parseSingleLine(%s) error", string(iMsg)))
    }
    iRes2, err := parseSingleLine(iMsg2)
    if iRes2 != nil || err == nil {
        t.Error(fmt.Sprintf("parseSingleLine(%s) error. Except nil and error, but get %v, %v", string(iMsg2), iRes2, err))
    }
}

func TestReadLine(t *testing.T) {
    state := new(readState)

    // test normal line
    data := []byte("+111\r\n-222\r\n:333\r\n")
    tem := bytes.NewReader(data)
    reader := bufio.NewReader(tem)
    for i := 1; ; i++ {
        line, err := readLine(reader, state)
        if err != nil {
            if err == io.EOF {
                break
            } else {
                t.Error("read data not stop normally")
            }
        }
        if !bytes.Equal(line, data[(i-1)*6:i*6]) {
            t.Error(fmt.Sprintf("readLine() == %v | expect: %v", line, data[(i-1)*6:i*6]))
        }
    }

    //     test bulk line
    state.bulkLen = 7
    state.multiLine = true
    data = []byte("1\r\n2\n34\r\n\r\n")
    reader = bufio.NewReader(bytes.NewReader(data))
    line, err := readLine(reader, state)
    if err != nil {
        t.Error(err)
    }
    if !bytes.Equal(line, data[:len(data)-2]) {
        t.Error(fmt.Sprintf("readLine() == %v | expect: %v", line, data[:len(data)-2]))
    }
    state.bulkLen = 0
    line, err = readLine(reader, state)
    if err != nil {
        t.Error(err)
    }
    if !bytes.Equal(line, data[len(data)-2:]) {
        t.Error(fmt.Sprintf("readLine() == %v | expect: %v", line, data[len(data)-2:]))
    }
}

func TestParseStream(t *testing.T) {
    var data []byte
    var reader io.Reader
    var ch <-chan *ParsedRes
    // test 1: Null elements in Arrays
    data = []byte("*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n")
    reader = bytes.NewReader(data)
    ch = ParseStream(reader)
    for parseRes := range ch {
        if parseRes.Err != nil {
            if parseRes.Err != io.EOF {
                t.Error(parseRes.Err)
            }
            break
        }
        array := parseRes.Data.(*ArrayData)
        for i, bulk := range array.Data() {
            switch i {
            case 0:
                if !bytes.Equal(bulk.(*BulkData).Data(), []byte("hello")) {
                    t.Error("parse hello error")
                }
            case 1:
                if bulk.(*BulkData).Data() != nil {
                    t.Error("parse nil error")
                }
            case 3:
                if !bytes.Equal(bulk.(*BulkData).Data(), []byte("world")) {
                    t.Error("parse world error")
                }
            }
        }
    }

    // test 2: null array
    data = []byte("*-1\r\n")
    reader = bytes.NewReader(data)
    ch = ParseStream(reader)
    for parseRes := range ch {
        if parseRes.Err != nil {
            if parseRes.Err != io.EOF {
                t.Error(parseRes.Err)
            }
            break
        }
        array := parseRes.Data.(*ArrayData)
        if array.Data() != nil || !bytes.Equal(array.ToBytes(), []byte("*-1\r\n")) {
            t.Error("parse nil array error: ")
            t.Error(fmt.Sprintf("get %v, %v | expect: nil, %v", array.Data(), array.ToBytes(), []byte("*-1\r\n")))
        }
    }

    //test 3: zero array
    data = []byte("*0\r\n")
    reader = bytes.NewReader(data)
    ch = ParseStream(reader)
    for parseRes := range ch {
        if parseRes.Err != nil {
            if parseRes.Err != io.EOF {
                t.Error(parseRes.Err)
            }
            break
        }
        array := parseRes.Data.(*ArrayData)
        if len(array.Data()) != 0 || !bytes.Equal(array.ToBytes(), []byte("*0\r\n")) {
            t.Error("parse nil array error: ")
            t.Error(fmt.Sprintf("get %v, %v | expect: 0, %v", len(array.Data()), array.ToBytes(), []byte("*0\r\n")))
        }
    }

    // test 4: nested array
    data = []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n")
    reader = bytes.NewReader(data)
    ch = ParseStream(reader)
    k := 0
    for parseRes := range ch {
        if parseRes.Err != nil {
            if parseRes.Err != io.EOF {
                t.Error(parseRes.Err)
            }
            break
        }
        array := parseRes.Data.(*ArrayData)
        if k == 0 { // first array
            for i, intData := range array.Data() {
                data := intData.(*IntData)
                if data.Data() != int64(i+1) || !bytes.Equal(data.ToBytes(), []byte(fmt.Sprintf(":%d\r\n", i+1))) {
                    t.Error("parse nested array error: ")
                    t.Error(fmt.Sprintf("get %v, %v | expect: %v, %v", data.Data(), data.ToBytes(), int64(i+1), []byte(fmt.Sprintf(":%d\r\n", i+1))))
                }
            }
        }
        if k == 1 { // second array
            for i, v := range array.Data() {
                if i == 0 {
                    data := v.(*StringData)
                    if data.Data() != "Hello" || !bytes.Equal(data.ToBytes(), []byte("+Hello\r\n")) {
                        t.Error(fmt.Sprintf("get %s, %v | expect: %s, %v", data.Data(), data.ToBytes(), "Hello", []byte("+Hello\r\n")))
                    }
                }
                if i == 1 {
                    data := v.(*ErrorData)
                    if data.Error() != "World" || !bytes.Equal(data.ToBytes(), []byte("-World\r\n")) {
                        t.Error(fmt.Sprintf("get %s, %v | expect: %s, %v", data.Error(), data.ToBytes(), "Hello", []byte("+Hello\r\n")))
                    }
                }
            }
        }
        k++

    }

    // test 5: bulk strings
    data = []byte("$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n")
    reader = bytes.NewReader(data)
    ch = ParseStream(reader)
    k = 0
    for parseRes := range ch {
        if parseRes.Err != nil {
            if parseRes.Err != io.EOF {
                t.Error(parseRes.Err)
            }
            break
        }
        bulk := parseRes.Data.(*BulkData)
        if k == 0 {
            if !bytes.Equal(bulk.Data(), []byte("hello")) || !bytes.Equal(bulk.ToBytes(), []byte("$5\r\nhello\r\n")) {
                t.Error(fmt.Sprintf("get %v, %v | expect: %v, %v", bulk.Data(), bulk.ToBytes(), []byte("hello"), []byte("$5\r\nhello\r\n")))
            }
        }
        if k == 1 {
            if bulk.Data() != nil || !bytes.Equal(bulk.ToBytes(), []byte("$-1\r\n")) {
                t.Error(fmt.Sprintf("get %v, %v | expect: nil, %v", bulk.Data(), bulk.ToBytes(), []byte("$-1\r\n")))
            }
        }
        if k == 2 {
            if !bytes.Equal(bulk.Data(), []byte("world")) || !bytes.Equal(bulk.ToBytes(), []byte("$5\r\nworld\r\n")) {
                t.Error(fmt.Sprintf("get %v, %v | expect: %v, %v", bulk.Data(), bulk.ToBytes(), []byte("world"), []byte("$5\r\nworld\r\n")))
            }
        }
        k++
    }
}
