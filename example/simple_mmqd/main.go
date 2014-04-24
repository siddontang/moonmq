package main

import (
	"github.com/siddontang/moonmq/broker"
)

func main() {
	cfg := broker.NewDefaultConfig()

	app, err := broker.NewAppWithConfig(cfg)
	if err != nil {
		panic(err)
	}

	app.Run()
}
