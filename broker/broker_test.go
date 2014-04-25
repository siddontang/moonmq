package broker

import (
	"bytes"
	"fmt"
	"github.com/siddontang/moonmq/client"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"
)

var testClientConfig = []byte(`
    {
        "broker_addr":"127.0.0.1:11181",
        "keepavlie":60,
        "idle_conns":16
    }
    `)

var testUrlMsg = "http://127.0.0.1:11180/msg"

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

	ch, err := c.Bind("test_queue", "", true)
	if err != nil {
		t.Fatal(err)
	}

	msg := ch.GetMsg()
	if string(msg) != "hello world" {
		t.Fatal(string(msg))
	}
}

func TestPubDirect(t *testing.T) {
	c1 := getClientConn()
	c2 := getClientConn()

	defer c1.Close()
	defer c2.Close()

	var ch1 *client.Channel
	var ch2 *client.Channel
	var err error

	if ch1, err = c1.Bind("test_queue", "a", true); err != nil {
		t.Fatal(err)
	}

	if ch2, err = c2.Bind("test_queue", "a", true); err != nil {
		t.Fatal(err)
	}

	wait := make(chan int, 2)
	f := func(id int, ch *client.Channel) {
		msg := ch.GetMsg()
		if string(msg) != "hello world" {
			println(string(msg), "error")
			t.Fatal(string(msg))
		}

		wait <- id
	}

	go f(1, ch1)
	go f(2, ch2)

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

			if i == 2 {
				return
			}
			i++
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

	var ch1 *client.Channel
	var ch2 *client.Channel
	var err error

	if ch1, err = c1.Bind("test_queue", "b", true); err != nil {
		t.Fatal(err)
	}

	if ch2, err = c2.Bind("test_queue", "b", true); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "b", []byte("hello world"), "fanout"); err != nil {
		t.Fatal(err)
	}

	if msg := ch1.GetMsg(); string(msg) != "hello world" {
		t.Fatal(string(msg))
	}

	if msg := ch2.GetMsg(); string(msg) != "hello world" {
		t.Fatal(string(msg))
	}
}

func TestUnbind(t *testing.T) {
	c := getClientConn()
	defer c.Close()

	var ch *client.Channel
	var err error
	if ch, err = c.Bind("test_queue", "c", true); err != nil {
		t.Fatal(err)
	}

	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "c", []byte("123"), "direct"); err != nil {
		t.Fatal(err)
	}

	if msg := ch.WaitMsg(1 * time.Second); msg != nil {
		t.Fatal(string(msg))
	}

	if ch, err = c.Bind("test_queue", "c", true); err != nil {
		t.Fatal(err)
	}

	if msg := ch.GetMsg(); string(msg) != "123" {
		t.Fatal(string(msg))
	}
}

func TestAck(t *testing.T) {
	c := getClientConn()
	defer c.Close()

	var ch *client.Channel
	var err error
	if ch, err = c.Bind("test_queue", "d", false); err != nil {
		t.Fatal(err)
	}

	if err := testPublish("test_queue", "d", []byte("123"), "direct"); err != nil {
		t.Fatal(err)
	}

	if msg := ch.GetMsg(); string(msg) != "123" {
		t.Fatal(string(msg))
	} else {
		if err := ch.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func testHttpPublish(queue string, routingKey string, body []byte, pubType string) error {
	url := fmt.Sprintf("%s?queue=%s&routing_key=%s&pub_type=%s", testUrlMsg, queue, routingKey, pubType)
	resp, err := http.Post(url, "text/plain", bytes.NewReader(body))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("publish error %s", resp.Status)
	}

	return nil
}

func testHttpConsume(queue string, routingKey string) ([]byte, error) {
	url := fmt.Sprintf("%s?queue=%s&routing_key=%s", testUrlMsg, queue, routingKey)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("consume error %s", resp.Status)
	}

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	} else {
		return ioutil.ReadAll(resp.Body)
	}
}

func TestHttp(t *testing.T) {
	if err := testHttpPublish("test_queue_http", "a", []byte("hello world"), "direct"); err != nil {
		t.Fatal(err)
	}

	if body, err := testHttpConsume("test_queue_http", "a"); err != nil {
		t.Fatal(err)
	} else if body == nil {
		t.Fatal("must have data")
	} else if string(body) != "hello world" {
		t.Fatal(string(body))
	}
}
