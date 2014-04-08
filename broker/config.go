package broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	Version uint32 `json:"version"`

	ListenAddrs []string `json:"listen_addrs"`

	Password string `json:"password"`

	KeepAlive int `json:"keepalive"`

	MaxMessageSize int `json:"max_msg_size"`
	MessageTimeout int `json:"msg_timeout"`
	MaxQueueSize   int `json:"max_queue_size"`

	Store       string          `json:"store"`
	StoreConfig json.RawMessage `json:"store_config"`
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
