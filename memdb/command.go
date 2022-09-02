package memdb

import "github.com/VincentFF/simpleredis/resp"

// CmdTable holds all registered commands
var CmdTable = make(map[string]*command)

type cmdExecutor func(m *MemDb, cmd [][]byte) resp.RedisData

type command struct {
    executor cmdExecutor
}

func RegisterCommand(cmdName string, executor cmdExecutor) {
    CmdTable[cmdName] = &command{
        executor: executor,
    }
}
