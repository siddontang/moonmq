package main

import (
	"flag"
	"github.com/siddontang/moonmq/client"
)

var addr = flag.String("addr", "127.0.0.1:11181", "moonmq listen address")
var queue = flag.String("queue", "test_queue", "queue want to bind")

func main() {
	flag.Parse()

	cfg := client.NewDefaultConfig()
	cfg.BrokerAddr = *addr

	c, err := client.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var conn *client.Conn
	conn, err = c.Get()
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	var ch *client.Channel
	ch, err = conn.Bind(*queue, "", true)

	msg := ch.GetMsg()
	println("get msg: ", string(msg))
}
