package memdb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VincentFF/thinredis/logger"
	"github.com/VincentFF/thinredis/resp"
)

// hash.go file implements the hash commands of redis
func hDelHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hdel" {
		logger.Error("hDelHash: command name is not hdel")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hdel' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}

	m.locks.Lock(key)
	defer m.locks.UnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if hash.IsEmpty() {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	res := 0
	for i := 2; i < len(cmd); i++ {
		res += hash.Del(string(cmd[i]))
	}

	return resp.MakeIntData(int64(res))
}

func hExistsHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hexists" {
		logger.Error("hExistsHash: command name is not hexists")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hexists' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if hash.Exist(string(cmd[2])) {
		return resp.MakeIntData(1)
	}

	return resp.MakeIntData(0)
}

func hGetHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hget" {
		logger.Error("hGetHash: command name is not hget")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hget' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	res := hash.Get(string(cmd[2]))
	if len(res) == 0 {
		return resp.MakeBulkData(nil)
	}
	return resp.MakeBulkData(res)
}

func hGetAllHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hgetall" {
		logger.Error("hGetAllHash: command name is not hgetall")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hgetall' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeEmptyArrayData()
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeEmptyArrayData()
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	table := hash.Table()
	res := make([]resp.RedisData, 0, len(table)*2)
	for k, v := range table {
		res = append(res, resp.MakeBulkData([]byte(k)), resp.MakeBulkData(v))
	}
	return resp.MakeArrayData(res)
}

func hIncrByHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hincrby" {
		logger.Error("hIncrByHash: command name is not hincrby")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hincrby' command")
	}

	var incr int
	var err error
	var hash *Hash
	key := string(cmd[1])
	field := string(cmd[2])

	incr, err = strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("incr value must be an integer")
	}

	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.UnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		hash = NewHash()
		m.db.Set(key, hash)
	} else {
		hash, ok = tem.(*Hash)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	res, ok := hash.IncrBy(field, incr)
	if !ok {
		return resp.MakeErrorData("value is not an integer")
	}
	return resp.MakeIntData(int64(res))
}

func hIncrByFloatHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hincrbyfloat" {
		logger.Error("hIncrByFloatHash: command name is not hincrbyfloat")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hincrbyfloat' command")
	}

	var hash *Hash
	key, field := string(cmd[1]), string(cmd[2])
	incr, err := strconv.ParseFloat(string(cmd[3]), 64)
	if err != nil {
		return resp.MakeErrorData("incr value must be a float")
	}
	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.UnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		hash = NewHash()
		m.db.Set(key, hash)
	} else {
		hash, ok = tem.(*Hash)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	res, ok := hash.IncrByFloat(field, incr)
	if !ok {
		return resp.MakeErrorData("value is not a float")
	}

	return resp.MakeBulkData([]byte(strconv.FormatFloat(res, 'f', -1, 64)))
}

func hKeysHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hkeys" {
		logger.Error("hKeysHash: command name is not hkeys")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hkeys' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeEmptyArrayData()
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeEmptyArrayData()
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	fields := hash.Keys()
	res := make([]resp.RedisData, 0, len(fields))
	for _, v := range fields {
		res = append(res, resp.MakeBulkData([]byte(v)))
	}
	return resp.MakeArrayData(res)
}

func hLenHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hlen" {
		logger.Error("hLenHash: command name is not hlen")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hlen' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.Len()
	return resp.MakeIntData(int64(res))
}

func hMGetHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hmget" {
		logger.Error("hMGetHash: command name is not hmget")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hmget' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeEmptyArrayData()
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeEmptyArrayData()
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := make([]resp.RedisData, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		field := string(cmd[i])
		data := hash.Get(field)
		if len(data) == 0 {
			res = append(res, resp.MakeBulkData(nil))
		} else {
			res = append(res, resp.MakeBulkData(data))
		}
	}
	return resp.MakeArrayData(res)
}

func hSetHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hset" {
		logger.Error("hMSetHash: command name is not hset")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) < 4 || len(cmd)&1 == 1 {
		return resp.MakeErrorData("wrong number of arguments for 'hset' command")
	}

	key := string(cmd[1])
	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.UnLock(key)

	var hash *Hash

	tem, ok := m.db.Get(key)
	if !ok {
		hash = NewHash()
		m.db.Set(key, hash)
	} else {
		hash, ok = tem.(*Hash)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	for i := 2; i < len(cmd); i += 2 {
		field := string(cmd[i])
		value := cmd[i+1]
		hash.Set(field, value)
	}
	return resp.MakeStringData("OK")
}

func hSetNxHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hsetnx" {
		logger.Error("hSetNxHash: command name is not hsetnx")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hsetnx' command")
	}

	key := string(cmd[1])
	field := string(cmd[2])
	value := cmd[3]

	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.UnLock(key)

	var hash *Hash
	tem, ok := m.db.Get(key)
	if !ok {
		hash = NewHash()
		m.db.Set(key, hash)
	} else {
		hash, ok = tem.(*Hash)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	if hash.Exist(field) {
		return resp.MakeIntData(0)
	}

	hash.Set(field, value)
	return resp.MakeIntData(1)
}

func hValsHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hvals" {
		logger.Error("hValsHash: command name is not hvals")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hvals' command")
	}

	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeEmptyArrayData()
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeEmptyArrayData()
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	vals := hash.Values()
	res := make([]resp.RedisData, 0, len(vals))
	for _, val := range vals {
		res = append(res, resp.MakeBulkData(val))
	}
	return resp.MakeArrayData(res)
}

func hStrLenHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hstrlen" {
		logger.Error("hStrLenHash: command name is not hstrlen")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hstrlen' command")
	}

	key := string(cmd[1])
	field := string(cmd[2])

	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	res := hash.StrLen(field)

	return resp.MakeIntData(int64(res))
}

func hRandFieldHash(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "hrandfield" {
		logger.Error("hRandFieldHash: command name is not hrandfield")
		return resp.MakeErrorData("server error")
	}

	if len(cmd) != 2 && len(cmd) != 3 && len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hrandfield' command")
	}

	key := string(cmd[1])
	count := 1
	withValues := false
	var err error

	if len(cmd) >= 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("err: count value must be integer")
		}
	}

	if len(cmd) == 4 {
		if strings.ToLower(string(cmd[3])) == "withvalues" {
			withValues = true
		} else {
			return resp.MakeErrorData(fmt.Sprintf("command option error, not support %s option", string(cmd[3])))
		}
	}

	if !m.CheckTTL(key) {
		return resp.MakeEmptyArrayData()
	}

	m.locks.RLock(key)
	defer m.locks.RUnLock(key)

	tem, ok := m.db.Get(key)
	if !ok {
		return resp.MakeEmptyArrayData()
	}
	hash, ok := tem.(*Hash)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := make([]resp.RedisData, 0)
	if withValues {
		fields := hash.RandomWithValue(count)
		for _, v := range fields {
			res = append(res, resp.MakeBulkData(v))
		}
	} else {
		fields := hash.Random(count)
		for _, v := range fields {
			res = append(res, resp.MakeBulkData([]byte(v)))
		}
	}
	return resp.MakeArrayData(res)
}

func RegisterHashCommands() {
	RegisterCommand("hdel", hDelHash)
	RegisterCommand("hexists", hExistsHash)
	RegisterCommand("hget", hGetHash)
	RegisterCommand("hgetall", hGetAllHash)
	RegisterCommand("hincrby", hIncrByHash)
	RegisterCommand("hincrbyfloat", hIncrByFloatHash)
	RegisterCommand("hkeys", hKeysHash)
	RegisterCommand("hlen", hLenHash)
	RegisterCommand("hmget", hMGetHash)
	RegisterCommand("hset", hSetHash)
	RegisterCommand("hsetnx", hSetNxHash)
	RegisterCommand("hvals", hValsHash)
	RegisterCommand("hstrlen", hStrLenHash)
	RegisterCommand("hrandfield", hRandFieldHash)
}
