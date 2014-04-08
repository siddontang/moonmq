package broker

import (
	"github.com/siddontang/moonmq/client"
	"sync"
	"testing"
	"time"
)

var testClientConfig = []byte(`
    {
        "broker_addr":"127.0.0.1:11181",
        "keepavlie":60,
        "password":"admin",
        "idle_conns":16
    }
    `)

var testClient *client.Client
var testClientOnce sync.Once

func getTestClient() *client.Client {
	f := func() {
		var err error
		testClient, err = client.NewClient(testClientConfig)
		if err != nil {
			println("------------", err.Error())
			panic(err)
		}
	}

	testClientOnce.Do(f)

	getTestApp()

	return testClient
}

func getClientConn() *client.Conn {
	client := getTestClient()

	c, err := client.Get()
	if err != nil {
		panic(err)
	}

	return c
}

func testPublish(queue string, routingKey string, body []byte, pubType string) error {
	c := getClientConn()
	defer c.Close()

	_, err := c.Publish(queue, routingKey, body, pubType)
	return err
}

func TestPush(t *testing.T) {
	if err := testPublish("test_queue", "", []byte("hello world"), "direct"); err != nil {
		t.Fatal(err)
	}

	c := getClientConn()
	defer c.Close()

	err := c.Bind("test_queue", nil, true)
	if err != nil {
		t.Fatal(err)
	}

	msg := c.GetMsg()
	if string(msg) != "hello world" {
		t.Fatal(string(msg))
	}
}

func TestPubDirect(t *testing.T) {
	c1 := getClientConn()
	c2 := getClientConn()

	defer c1.Close()
	defer c2.Close()

	if err := c1.Bind("test_queue", []string{"a"}, true); err != nil {
		t.Fatal(err)
	}

	if err := c2.Bind("test_queue", []string{"a"}, true); err != nil {
		t.Fatal(err)
	}

	wait := make(chan int, 2)
	f := func(id int, c *client.Conn) {
		msg := c.GetMsg()
		if string(msg) != "hello world" {
			println(string(msg), "error")
			t.Fatal(string(msg))
		}

		wait <- id
	}

	go f(1, c1)
	go f(2, c2)

	if err := testPublish("test_queue", "a", []byte("hello world"), "direct"); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "a", []byte("hello world"), "direct"); err != nil {
		t.Fatal(err)
	}

	i := 1
	for {
		select {
		case id := <-wait:
			if i != id {
				t.Fatal(i)
			}

			i++
			if i == 2 {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}

	}

}

func TestPubFanout(t *testing.T) {
	c1 := getClientConn()
	c2 := getClientConn()

	defer c1.Close()
	defer c2.Close()

	if err := c1.Bind("test_queue", []string{"b"}, true); err != nil {
		t.Fatal(err)
	}

	if err := c2.Bind("test_queue", []string{"b"}, true); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "b", []byte("hello world"), "fanout"); err != nil {
		t.Fatal(err)
	}

	if msg := c1.GetMsg(); string(msg) != "hello world" {
		t.Fatal(string(msg))
	}

	if msg := c2.GetMsg(); string(msg) != "hello world" {
		t.Fatal(string(msg))
	}
}

func TestUnbind(t *testing.T) {
	c := getClientConn()
	defer c.Close()

	if err := c.Bind("test_queue", []string{"c"}, true); err != nil {
		t.Fatal(err)
	}

	if err := c.Unbind("test_queue"); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "c", []byte("123"), "direct"); err != nil {
		t.Fatal(err)
	}

	if msg := c.WaitMsg(1); msg != nil {
		t.Fatal(string(msg))
	}

	if err := c.Bind("test_queue", []string{"c"}, true); err != nil {
		t.Fatal(err)
	}

	if msg := c.GetMsg(); string(msg) != "123" {
		t.Fatal(string(msg))
	}
}

func TestAck(t *testing.T) {
	c := getClientConn()
	defer c.Close()

	if err := c.Bind("test_queue", []string{"d"}, false); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "d", []byte("123"), "direct"); err != nil {
		t.Fatal(err)
	}

	if msg := c.GetMsg(); string(msg) != "123" {
		t.Fatal(string(msg))
	}
}
