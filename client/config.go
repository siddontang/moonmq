package client

import (
	"encoding/json"
)

const defaultQueueSize int = 16

type Config struct {
	BrokerAddr   string `json:"broker_addr"`
	KeepAlive    int    `json:"keepalive"`
	IdleConns    int    `json:"idle_conns"`
	MaxQueueSize int    `json:"max_queue_size"`
}

func NewDefaultConfig() *Config {
	cfg := new(Config)

	cfg.BrokerAddr = "127.0.0.1:11181"
	cfg.KeepAlive = 60
	cfg.IdleConns = 2
	cfg.MaxQueueSize = 16

	return cfg
}

func parseConfigJson(buf json.RawMessage) (*Config, error) {
	c := new(Config)

	if err := json.Unmarshal(buf, c); err != nil {
		return nil, err
	}

	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = defaultQueueSize
	}

	return c, nil
}
