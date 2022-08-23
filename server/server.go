package server

import (
	"github.com/VincentFF/simpleredis/config"
	"github.com/VincentFF/simpleredis/logger"
	"net"
	"strconv"
)

func Start(cfg *config.Config) error {
	listener, err := net.Listen("tcp", cfg.Host+":"+strconv.Itoa(cfg.Port))
	if err != nil {
		logger.Panic(err)
		return err
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err)
			conn.Close()
		} else {
			go Handle(conn)
		}
	}
	return nil
}
func Handle(conn net.Conn) {

}