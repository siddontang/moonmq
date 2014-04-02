package client

import (
	"encoding/json"
)

type Config struct {
	BrokerNet  string `json:"broker_net"`
	BrokerAddr string `json:"broker_addr"`
	KeepAlive  int    `json:"keepalive"`
	Password   string `json:"password"`
	IdleConns  int    `json:"idle_conns"`
}

func parseConfigJson(buf json.RawMessage) (*Config, error) {
	c := new(Config)

	if err := json.Unmarshal(buf, c); err != nil {
		return nil, err
	}

	return c, nil
}
