package config

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ConfFile string
	Host     string
	Port     int
	LogDir   string
	LogLevel string
}

type CfgError struct {
	message string
}

func (cErr *CfgError) Error() string {
	return cErr.message
}

func flagInit(cfg *Config) {
	flag.StringVar(&(cfg.ConfFile), "config", "", "Appoint a config file: such as /etc/redis.conf")
	flag.StringVar(&(cfg.Host), "host", "127.0.0.1", "Bind host ip: default is 127.0.0.1")
	flag.IntVar(&(cfg.Port), "port", 6379, "Bind a listening port: default is 6399")
	flag.StringVar(&(cfg.LogDir), "logdir", "", "Set log directory: default is /tmp")
	flag.StringVar(&(cfg.LogLevel), "loglevel", "info", "Set log level: default is info")
}

// Setup initialize configs and do some validation checking.
// Return configured Config pointer and error.
func Setup() (*Config, error) {
	cfg := new(Config)
	flagInit(cfg)
	flag.Parse()
	if cfg.ConfFile != "" {
		if err := cfg.Parse(cfg.ConfFile); err != nil {
			return nil, err
		}
		return cfg, nil
	} else {
		if ip := net.ParseIP(cfg.Host); ip == nil {
			ipErr := &CfgError{
				message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
			}
			return nil, ipErr
		}
		if cfg.Port <= 1024 || cfg.Port >= 65535 {
			portErr := &CfgError{
				message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", cfg.Port),
			}
			return nil, portErr
		}
	}
	return cfg, nil
}

// Parse is used to parse the config file and return error
func (cfg *Config) Parse(cfgFile string) error {
	fl, err := os.Open(cfgFile)
	if err != nil {
		return err
	}
	defer fl.Close()

	reader := bufio.NewReader(fl)
	for {
		line, ioErr := reader.ReadString('\n')
		if ioErr != nil && ioErr != io.EOF {
			return ioErr
		}
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		cfgName := strings.ToLower(fields[0])
		if cfgName == "host" {
			if ip := net.ParseIP(fields[1]); ip == nil {
				ipErr := &CfgError{
					message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
				}
				return ipErr
			}
			cfg.Host = fields[1]
		} else if cfgName == "port" {
			port, err := strconv.Atoi(fields[1])
			if err != nil {
				return err
			}
			if port <= 1024 || port >= 65535 {
				portErr := &CfgError{
					message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", port),
				}
				return portErr
			}
			cfg.Port = port
		} else if cfgName == "logdir" {
			cfg.LogDir = strings.ToLower(fields[1])
		} else if cfgName == "loglevel" {
			cfg.LogLevel = strings.ToLower(fields[1])
		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}
