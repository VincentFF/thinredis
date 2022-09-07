package main

import (
	"fmt"
	"os"

	"github.com/VincentFF/thinredis/config"
	"github.com/VincentFF/thinredis/logger"
	"github.com/VincentFF/thinredis/memdb"
	"github.com/VincentFF/thinredis/server"
)

func init() {
	// Register commands
	memdb.RegisterKeyCommands()
	memdb.RegisterStringCommands()
	memdb.RegisterListCommands()
	memdb.RegisterSetCommands()
	memdb.RegisterHashCommands()
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
