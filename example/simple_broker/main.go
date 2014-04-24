package main

import (
	"flag"
	"github.com/siddontang/moonmq/broker"
)

var addr = flag.String("addr", "127.0.0.1:11181", "moonmq broker listen address")

func main() {
	flag.Parse()

	cfg := broker.NewDefaultConfig()
	cfg.Addr = *addr

	app, err := broker.NewAppWithConfig(cfg)
	if err != nil {
		panic(err)
	}

	app.Run()
}
