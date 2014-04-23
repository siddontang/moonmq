package client

import (
	"encoding/json"
)

const defaultQueueSize int = 16

type Config struct {
	BrokerAddr   string `json:"broker_addr"`
	KeepAlive    int    `json:"keepalive"`
	Password     string `json:"password"`
	IdleConns    int    `json:"idle_conns"`
	MaxQueueSize int    `json:"max_queue_size"`
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
