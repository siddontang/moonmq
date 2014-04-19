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

	listeners []net.Listener

	redis *redis.Pool

	ms Store

	qs *queues

	passMD5 []byte
}

func NewApp(jsonConfig json.RawMessage) (*App, error) {
	app := new(App)

	var err error
	var cfg *Config
	if cfg, err = parseConfigJson(jsonConfig); err != nil {
		return nil, err
	}

	app.cfg = cfg

	if len(cfg.Password) > 0 {
		sum := md5.Sum([]byte(cfg.Password))
		app.passMD5 = sum[0:16]
	}

	app.listeners = make([]net.Listener, len(cfg.ListenAddrs))

	for i, a := range cfg.ListenAddrs {
		var n string = "tcp"
		if strings.Contains(a, "/") {
			n = "unix"
		}

		app.listeners[i], err = net.Listen(n, a)
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

func (app *App) Config() *Config {
	return app.cfg
}

func (app *App) Close() {
	for _, l := range app.listeners {
		l.Close()
	}

	app.ms.Close()
}

func (app *App) Run() {
	l := app.listeners[0]

	for i := 1; i < len(app.listeners); i++ {
		go app.listen(l)
	}

	app.listen(l)
}

func (app *App) listen(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		co := newConn(app, conn)
		go co.run()
	}
}
