package main

import (
    "fmt"
    "os"

    "github.com/VincentFF/simpleredis/config"
    "github.com/VincentFF/simpleredis/logger"
    "github.com/VincentFF/simpleredis/memdb"
    "github.com/VincentFF/simpleredis/server"
)

func init() {
    // Register commands
    memdb.RegisterKeyCommands()
    memdb.RegisterStringCommands()
    memdb.RegisterListCommands()
}

func main() {
    cfg, err := config.Setup()
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    err = logger.SetUp(cfg)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    err = server.Start(cfg)
    if err != nil {
        os.Exit(1)
    }
}
