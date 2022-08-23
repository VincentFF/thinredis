package main

import (
	"fmt"
	"github.com/VincentFF/simpleredis/config"
	"github.com/VincentFF/simpleredis/logger"
	"github.com/VincentFF/simpleredis/server"
	"os"
)

func main() {
	cfg, err := config.Setup()
	fmt.Println(cfg)
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
