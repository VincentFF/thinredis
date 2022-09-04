package memdb

import (
    "strings"

    "github.com/VincentFF/simpleredis/logger"
    "github.com/VincentFF/simpleredis/resp"
)

// list.go file implements the list commands of redis

func lLenList(m *MemDb, cmd [][]byte) resp.RedisData {
    if strings.ToLower(string(cmd[0])) != "llen" {
        logger.Error("lLenList Function: cmdName is not llen")
        return resp.MakeErrorData("server error")
    }

    if len(cmd) != 2 {
        return resp.MakeErrorData("wrong number of arguments for 'llen' command")
    }

    key := string(cmd[1])

    m.locks.RLock(key)
    defer m.locks.RUnlock(key)

    v, ok := m.db.Get(key)
    if !ok {
        return resp.MakeIntData(0)
    }

    typeV, ok := v.(List)
    if !ok {
        return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
    }

    return resp.MakeIntData(int64(typeV.Len()))
}

func lPopList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func rPopList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lPushList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lPushXList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func rPushList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func rPushXList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lIndexList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lSetList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lRemList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lTrimList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func blPopList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func brPopList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func lRangeList(m *MemDb, cmd [][]byte) resp.RedisData {
    return nil
}

func RegisterListCommands() {
    RegisterCommand("llen", lLenList)
    RegisterCommand("lpop", lPopList)
    RegisterCommand("rpop", rPopList)
    RegisterCommand("lpush", lPushList)
    RegisterCommand("lpushx", lPushXList)
    RegisterCommand("rpush", rPushList)
    RegisterCommand("rpushx", rPushXList)
    RegisterCommand("lindex", lIndexList)
    RegisterCommand("lset", lSetList)
    RegisterCommand("lrem", lRemList)
    RegisterCommand("ltrim", lTrimList)
    RegisterCommand("blpop", blPopList)
    RegisterCommand("brpop", brPopList)
    RegisterCommand("lrange", lRangeList)
}
