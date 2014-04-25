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
        "addr": "127.0.0.1:11181",

        "http_addr": "127.0.0.1:11180",

        "keepalive":60,

        "max_msg_size":1024,
        "msg_timeout":10,
        "max_queue_size":1024,

        "store":"redis",
        "store_config": {
            "addr":"127.0.0.1:6379",
            "db":0,
            "password":"",
            "idle_conns":16,
            "key_prefix":"test_moonmq"
        }
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
