package broker

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"

	"github.com/siddontang/golib/timingwheel"
	"net"
	"time"
)

type App struct {
	cfg *Config

	listeners []net.Listener

	wheel *timingwheel.TimingWheel

	redis *redis.Pool

	ms *msgStore

	qs *queues
}

func NewApp(jsonConfig json.RawMessage) (*App, error) {
	app := new(App)

	var err error
	var cfg *Config
	if cfg, err = parseConfigJson(jsonConfig); err != nil {
		return nil, err
	}

	app.cfg = cfg

	app.listeners = make([]net.Listener, len(cfg.ListenAddrs))

	for i, a := range cfg.ListenAddrs {
		app.listeners[i], err = net.Listen(a.Net, a.Addr)
		if err != nil {
			return nil, err
		}
	}

	app.wheel = timingwheel.NewTimingWheel(time.Second, 3600)

	app.initRedis(cfg)

	app.ms = newMsgStore(app)

	app.qs = newQueues(app)

	return app, nil
}

func (app *App) initRedis(cfg *Config) {
	f := func() (redis.Conn, error) {
		c, err := redis.Dial(cfg.Redis.Net, cfg.Redis.Addr)
		if err != nil {
			return nil, err
		}

		if len(cfg.Redis.Password) > 0 {
			if _, err = c.Do("AUTH", cfg.Redis.Password); err != nil {
				c.Close()
				return nil, err
			}
		}

		if cfg.Redis.DB != 0 {
			if _, err = c.Do("SELECT", cfg.Redis.DB); err != nil {
				c.Close()
				return nil, err
			}
		}

		return c, nil
	}

	app.redis = redis.NewPool(f, cfg.Redis.IdleConns)
}

func (app *App) Config() *Config {
	return app.cfg
}

func (app *App) Close() {
	for _, l := range app.listeners {
		l.Close()
	}
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
