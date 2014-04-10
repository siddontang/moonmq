package client

import (
	"container/list"
	"crypto/md5"
	"encoding/json"
	"github.com/siddontang/moonmq/proto"
	"sync"
	"time"
)

type Client struct {
	sync.Mutex

	cfg *Config

	passMD5 []byte

	conns *list.List
}

func NewClient(jsonConfig json.RawMessage) (*Client, error) {
	c := new(Client)

	cfg, err := parseConfigJson(jsonConfig)
	if err != nil {
		return nil, err
	}

	c.cfg = cfg

	if len(cfg.Password) > 0 {
		sum := md5.Sum([]byte(cfg.Password))
		c.passMD5 = sum[0:16]
	}

	c.conns = list.New()

	go c.run()

	return c, nil
}

func (c *Client) run() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ticker.C:
			if co := c.popConn(); co != nil {
				if err := co.keepalive(); err == nil {
					c.pushConn(co)
				}
			}
		}
	}
}

func (c *Client) Get() (*Conn, error) {
	co := c.popConn()
	if co != nil {
		return co, nil
	} else {
		return newConn(c)
	}
}

func (c *Client) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	conn, err := c.Get()
	if err != nil {
		return 0, err
	}

	defer conn.Close()

	return conn.Publish(queue, routingKey, body, pubType)
}

func (c *Client) PublishFanout(queue string, body []byte) (int64, error) {
	return c.Publish(queue, "", body, proto.FanoutTypeStr)
}

func (c *Client) PublishDirect(queue string, routingKey string, body []byte) (int64, error) {
	return c.Publish(queue, routingKey, body, proto.DirectTypeStr)
}

func (c *Client) popConn() *Conn {
	c.Lock()
	if c.conns.Len() == 0 {
		c.Unlock()
		return nil
	} else {
		e := c.conns.Front()
		c.conns.Remove(e)

		c.Unlock()

		return e.Value.(*Conn)
	}
}

func (c *Client) pushConn(co *Conn) {
	c.Lock()
	if c.conns.Len() >= c.cfg.IdleConns {
		c.Unlock()
		co.close()
	} else {
		c.conns.PushBack(co)
		c.Unlock()
	}
}
