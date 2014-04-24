package broker

import (
	"crypto/md5"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"net"
	"strings"
)

type App struct {
	cfg *Config

	listener net.Listener

	redis *redis.Pool

	ms Store

	qs *queues

	passMD5 []byte
}

func NewAppWithConfig(cfg *Config) (*App, error) {
	app := new(App)

	app.cfg = cfg

	var err error

	if len(cfg.Password) > 0 {
		sum := md5.Sum([]byte(cfg.Password))
		app.passMD5 = sum[0:16]
	}

	var n string = "tcp"
	if strings.Contains(cfg.Addr, "/") {
		n = "unix"
	}

	app.listener, err = net.Listen(n, cfg.Addr)
	if err != nil {
		return nil, err
	}

	app.qs = newQueues(app)

	app.ms, err = OpenStore(cfg.Store, cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func NewApp(jsonConfig json.RawMessage) (*App, error) {
	cfg, err := parseConfigJson(jsonConfig)
	if err != nil {
		return nil, err

	}

	return NewAppWithConfig(cfg)
}

func (app *App) Config() *Config {
	return app.cfg
}

func (app *App) Close() {
	app.listener.Close()
	app.ms.Close()
}

func (app *App) Run() {
	for {
		conn, err := app.listener.Accept()
		if err != nil {
			continue
		}

		co := newConn(app, conn)
		go co.run()
	}
}
