package broker

import (
	"sync"
	"testing"
)

var testOnce sync.Once
var testApp *App

var testConfig = `
    {
        "version":1,
        "listen_addrs":[
            {
                "net":"tcp",
                "addr":"127.0.0.1:11181"
            }
        ],

        "keepalive" : 60,

        "password": "admin",

        "redis": {
            "net":"tcp",
            "addr":"127.0.0.1:6379",
            "db":0,
            "password":"",
            "idle_conns":16
        },

        "key_prefix":"test_moonmq",
        "max_msg_size":1024,
        "msg_timeout":10
    }
`

func getTestApp() *App {
	f := func() {
		var err error
		testApp, err = NewApp([]byte(testConfig))
		if err != nil {
			println(err.Error())
		}

		go testApp.Run()
	}

	testOnce.Do(f)

	return testApp
}

func TestApp(t *testing.T) {
	getTestApp()
}
