package broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	Version uint32 `json:"version"`

	Addr string `json:"addr"`

	HttpAddr string `json:"http_addr"`

	KeepAlive int `json:"keepalive"`

	MaxMessageSize int `json:"max_msg_size"`
	MessageTimeout int `json:"msg_timeout"`
	MaxQueueSize   int `json:"max_queue_size"`

	Store       string          `json:"store"`
	StoreConfig json.RawMessage `json:"store_config"`
}

func NewDefaultConfig() *Config {
	cfg := new(Config)

	cfg.Version = 1
	cfg.Addr = "127.0.0.1:11181"
	cfg.HttpAddr = "127.0.0.1:11180"

	cfg.KeepAlive = 65

	cfg.MaxMessageSize = 1024
	cfg.MessageTimeout = 3600 * 24
	cfg.MaxQueueSize = 1024

	cfg.Store = "mem"
	cfg.StoreConfig = nil

	return cfg
}

func parseConfigJson(buf json.RawMessage) (*Config, error) {
	cfg := new(Config)

	err := json.Unmarshal(buf, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.KeepAlive > 600 {
		return nil, fmt.Errorf("keepalive must less than 600s, not %d", cfg.KeepAlive)
	}

	return cfg, nil
}

func parseConfigFile(configFile string) (*Config, error) {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	return parseConfigJson(buf)
}
