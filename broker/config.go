package broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	Version uint32 `json:"version"`

	ListenAddrs []struct {
		Net  string `json:"net"`
		Addr string `json:"addr"`
	} `json:"listen_addrs"`

	Password string `json:"password"`

	KeepAlive int `json:"keepalive"`

	Redis struct {
		Net       string `json:"net"`
		Addr      string `json:"addr"`
		DB        int    `json:"db"`
		Password  string `json:"password"`
		IdleConns int    `json:"idle_conns"`
	} `json:"redis"`

	KeyPrefix string `json:"key_prefix"`

	MaxMessageSize int `json:"max_msg_size"`
	MessageTimeout int `json:"msg_timeout"`
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
