package config

import (
    "fmt"
    "testing"
)

func TestConfig_Parse(t *testing.T) {
    cfg := new(Config)
    err := cfg.Parse("./test.conf")
    if err != nil {
        t.Error(err)
    }
    if cfg.Host != "127.0.0.1" {
        t.Error(fmt.Sprintf("cfg.Host == %s, expect 127.0.0.1", cfg.Host))
    }
    if cfg.Port != 6399 {
        t.Error(fmt.Sprintf("cfg.Port == %d, expect 6399", cfg.Port))
    }
    if cfg.LogDir != "/tmp" {
        t.Error(fmt.Sprintf("cfg.LogDir == %s, expect /tmp", cfg.LogDir))
    }
    if cfg.LogLevel != "info" {
        t.Error(fmt.Sprintf("cfg.LogLevel == %s, expect info", cfg.LogLevel))
    }
    if cfg.ShardNum != 1024 {
        t.Error(fmt.Sprintf("cfg.ShardNum == %d, expect 1024", cfg.ShardNum))
    }
}
