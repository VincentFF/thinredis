package logger

import (
	"fmt"
	"github.com/VincentFF/simpleredis/config"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

type LogLevel int
type LogConfig struct {
	Path  string
	Name  string
	Level LogLevel
}

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	PANIC
)

var (
	logFile     *os.File
	logger      *log.Logger
	logMu       sync.Mutex
	levelLabels = []string{"debug", "info", "warning", "error", "panic"}
	logcfg      *LogConfig
)

func SetUp(cfg *config.Config) error {
	var err error
	logcfg = &LogConfig{
		Path:  cfg.LogDir,
		Name:  "redis.log",
		Level: INFO,
	}
	for i, v := range levelLabels {
		if v == cfg.LogLevel {
			logcfg.Level = LogLevel(i)
			break
		}
	}

	if _, err = os.Stat(logcfg.Path); err != nil {
		mkErr := os.Mkdir(logcfg.Path, 0755)
		if mkErr != nil {
			return mkErr
		}
	}

	logfile := path.Join(logcfg.Path, logcfg.Name)
	logFile, err = os.OpenFile(logfile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	writer := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(writer, "", log.Ldate|log.Ltime|log.Lshortfile)
	return nil
}

func Debug(v ...any) {
	if logcfg.Level > DEBUG {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger.SetPrefix(fmt.Sprintf("%-10s", strings.ToUpper(levelLabels[DEBUG])))
	logger.Println(v)
}

func Info(v ...any) {
	if logcfg.Level > INFO {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger.SetPrefix(fmt.Sprintf("%-10s", strings.ToUpper(levelLabels[INFO])))

	logger.Println(v)
}

func Warning(v ...any) {
	if logcfg.Level > WARNING {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger.SetPrefix(fmt.Sprintf("%-10s", strings.ToUpper(levelLabels[WARNING])))
	logger.Println(v)
}

func Error(v ...any) {
	if logcfg.Level > ERROR {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger.SetPrefix(fmt.Sprintf("%-10s", strings.ToUpper(levelLabels[ERROR])))
	logger.Println(v)
}

func Panic(v ...any) {
	if logcfg.Level > PANIC {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger.SetPrefix(fmt.Sprintf("%-10s", strings.ToUpper(levelLabels[PANIC])))
	logger.Println(v)
}
