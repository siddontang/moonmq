package broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	BrokerID uint16 `json:"broker_id"`

	DataDir string `json:"data_dir"`

	Version uint32 `json:"version"`

	ListenAddr string `json:"listen_addr"`

	ErrorLog string `json:"error_log"`

	KeepAlive int `json:"keepalive"`
}

func parseConfig(configFile string) (*Config, error) {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)

	err = json.Unmarshal(buf, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.KeepAlive > 600 {
		return nil, fmt.Errorf("keepalive must less than 600s, not %d", cfg.KeepAlive)
	}

	return cfg, nil
}
