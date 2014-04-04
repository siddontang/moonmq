package client

import (
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Conn struct {
	sync.Mutex

	client *Client

	cfg *Config

	conn net.Conn

	decoder *proto.Decoder

	msg chan []byte

	grab chan struct{}
	wait chan *proto.Proto

	closed bool

	lastHeartbeat int64
}

func newConn(client *Client) (*Conn, error) {
	c := new(Conn)

	c.client = client
	c.cfg = client.cfg

	var n string = "tcp"
	if strings.Contains(c.cfg.BrokerAddr, "/") {
		n = "unix"
	}

	var err error
	if c.conn, err = net.Dial(n, c.cfg.BrokerAddr); err != nil {
		return nil, err
	}

	c.decoder = proto.NewDecoder(c.conn)

	c.msg = make(chan []byte, 1024)

	c.grab = make(chan struct{}, 1)
	c.grab <- struct{}{}

	c.wait = make(chan *proto.Proto, 1)

	c.closed = false

	c.lastHeartbeat = 0

	go c.run()

	if err = c.auth(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Conn) Close() {
	c.Unbind("")

	c.client.pushConn(c)
}

func (c *Conn) close() {
	c.conn.Close()
	c.closed = true
}

func (c *Conn) run() {
	defer func() {
		c.conn.Close()

		close(c.wait)

		c.closed = true
	}()
	for {
		p, err := c.decoder.Decode()
		if err != nil {
			return
		}

		if p.Method == proto.Push {
			if len(c.msg) >= 1024 {
				<-c.msg
			}

			c.ack(p)

			c.msg <- p.Body
		} else {
			c.wait <- p
		}

	}
}

func (c *Conn) request(p *proto.Proto, expectMethod uint32) (*proto.Proto, error) {
	<-c.grab

	err := c.writeProto(p)

	c.grab <- struct{}{}

	if err != nil {
		return nil, err
	}

	rp, ok := <-c.wait
	if !ok {
		return nil, fmt.Errorf("wait channel closed")
	}

	if rp.Method == proto.Error {
		return nil, fmt.Errorf("error:%v, code:%d", rp.Body, rp.Fields[proto.CodeStr])
	} else if rp.Method != expectMethod {
		return nil, fmt.Errorf("invalid return method %d != %d", rp.Method, expectMethod)
	}

	return rp, nil
}

func (c *Conn) writeProto(p *proto.Proto) error {
	buf, err := proto.Marshal(p)
	if err != nil {
		return err
	}
	c.Lock()
	n, err := c.conn.Write(buf)
	c.Unlock()

	if err != nil {
		c.conn.Close()
		return err
	} else if n != len(buf) {
		c.conn.Close()
		return fmt.Errorf("write short %d != %d", n, len(buf))
	}

	return nil
}

func (c *Conn) auth() error {
	if len(c.client.passMD5) == 0 {
		return nil
	}

	p := proto.NewAuthProto(c.client.passMD5)
	_, err := c.request(p.P, proto.Auth_OK)
	return err
}

func (c *Conn) ack(pushProto *proto.Proto) error {
	if pushProto.Value(proto.NoAckStr) == "1" {
		return nil
	}

	queue := pushProto.Queue()

	routingKey := pushProto.RoutingKey()
	msgId := pushProto.MsgId()

	p := proto.NewAckProto(queue, routingKey, msgId)

	return c.writeProto(p.P)
}

func (c *Conn) keepalive() error {
	n := time.Now().Unix()

	if n-c.lastHeartbeat < int64(float32(c.cfg.KeepAlive)*0.8) {
		return nil
	}

	p := proto.NewHeartbeatProto()

	return c.writeProto(p.P)
}

func (c *Conn) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	p := proto.NewPublishProto(queue, routingKey, pubType, body)

	np, err := c.request(p.P, proto.Publish_OK)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(np.Body), 10, 64)
}

func (c *Conn) Bind(queue string, routingKeys []string, noAck bool) error {
	p := proto.NewBindProto(queue, routingKeys, noAck)

	rp, err := c.request(p.P, proto.Bind_OK)

	if err != nil {
		return err
	}

	if rp.Queue() != queue {
		return fmt.Errorf("invalid bind response queue %s", rp.Queue())
	}

	return nil
}

func (c *Conn) Unbind(queue string) error {
	p := proto.NewUnbindProto(queue)

	rp, err := c.request(p.P, proto.Unbind_OK)
	if err != nil {
		return err
	}

	if rp.Queue() != queue {
		return fmt.Errorf("invalid bind response queue %s", rp.Queue())
	}

	return nil
}

func (c *Conn) GetMsg() []byte {
	return <-c.msg
}

func (c *Conn) WaitMsg(timeout int) []byte {
	select {
	case msg := <-c.msg:
		return msg
	case <-time.After(time.Duration(timeout) * time.Second):
		return nil
	}
}
