package broker

import (
	"github.com/siddontang/golib/log"
	"github.com/siddontang/golib/timingwheel"
	"net"
	"os"
	"time"
)

type App struct {
	cfg *Config

	listener net.Listener

	errLog *log.Logger

	wheel *timingwheel.TimingWheel
}

func NewApp(configFile string) (*App, error) {
	app := new(App)

	var err error
	var cfg *Config
	if cfg, err = parseConfig(configFile); err != nil {
		return nil, err
	}

	app.cfg = cfg

	if app.listener, err = net.Listen("tcp", cfg.ListenAddr); err != nil {
		return nil, err
	}

	if handler, err := log.NewFileHandler(cfg.ErrorLog, os.O_APPEND, 1024); err != nil {
		return nil, err
	} else {
		app.errLog = log.NewDefault(handler)
	}

	app.wheel = timingwheel.NewTimingWheel(time.Second, 3600)

	return app, nil
}

func (app *App) Config() *Config {
	return app.cfg
}

func (app *App) Close() {
	if app.listener != nil {
		app.listener.Close()
	}
}

func (app *App) Run() {
	for {
		conn, err := app.listener.Accept()
		if err != nil {
			continue
		}

		app.handleConn(conn)
	}
}

func (app *App) handleConn(c net.Conn) {
	co := newConn(app, c)
	go co.run()
}
