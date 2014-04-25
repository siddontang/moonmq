package broker

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"net"
	"net/http"
	"strings"
)

type App struct {
	cfg *Config

	listener net.Listener

	httpListener net.Listener

	redis *redis.Pool

	ms Store

	qs *queues

	passMD5 []byte
}

func NewAppWithConfig(cfg *Config) (*App, error) {
	app := new(App)

	app.cfg = cfg

	var err error

	app.listener, err = net.Listen(getNetType(cfg.Addr), cfg.Addr)
	if err != nil {
		return nil, err
	}

	if len(cfg.HttpAddr) > 0 {
		app.httpListener, err = net.Listen(getNetType(cfg.HttpAddr), cfg.HttpAddr)
		if err != nil {
			return nil, err
		}
	}

	app.qs = newQueues(app)

	app.ms, err = OpenStore(cfg.Store, cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func getNetType(addr string) string {
	if strings.Contains(addr, "/") {
		return "unix"
	} else {
		return "tcp"
	}
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
	if app.listener != nil {
		app.listener.Close()
	}

	if app.httpListener != nil {
		app.httpListener.Close()
	}

	app.ms.Close()
}

func (app *App) startHttp() {
	if app.httpListener == nil {
		return
	}

	s := new(http.Server)

	http.Handle("/msg", newMsgHandler(app))

	s.Serve(app.httpListener)
}

func (app *App) startTcp() {
	for {
		conn, err := app.listener.Accept()
		if err != nil {
			continue
		}

		co := newConn(app, conn)
		go co.run()
	}
}

func (app *App) Run() {
	go app.startHttp()

	app.startTcp()
}
