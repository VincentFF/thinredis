package server

import (
    "net"
    "strconv"
    "sync"

    "github.com/VincentFF/simpleredis/config"
    "github.com/VincentFF/simpleredis/logger"
)

func Start(cfg *config.Config) error {
    listener, err := net.Listen("tcp", cfg.Host+":"+strconv.Itoa(cfg.Port))
    if err != nil {
        logger.Panic(err)
        return err
    }
    defer func() {
        err := listener.Close()
        if err != nil {
            logger.Error(err)
        }
    }()

    logger.Info("Server Listen at ", cfg.Host, ":", cfg.Port)

    var sg sync.WaitGroup
    handler := NewHandler()
    for {
        conn, err := listener.Accept()
        if err != nil {
            logger.Error(err)
            break
        }

        logger.Info(conn.RemoteAddr().String(), " connected")
        sg.Add(1)
        go func() {
            defer sg.Done()
            handler.Handle(conn)
        }()
    }
    sg.Wait()
    return nil
}
