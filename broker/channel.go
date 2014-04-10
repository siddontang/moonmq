package broker

import (
	"github.com/siddontang/moonmq/proto"
	"strconv"
)

//use channel represent conn bind a queue

type channel struct {
	c          *conn
	q          *queue
	routingKey string
	noAck      bool
}

func newChannel(c *conn, q *queue, routingKey string, noAck bool) *channel {
	ch := new(channel)

	ch.c = c
	ch.q = q

	ch.routingKey = routingKey
	ch.noAck = noAck

	q.Bind(ch)

	return ch
}

func (c *channel) Reset(routingKey string, noAck bool) {
	c.routingKey = routingKey
	c.noAck = noAck
}

func (c *channel) Close() {
	c.q.Unbind(c)
}

func (c *channel) Push(m *msg) error {
	p := proto.NewPushProto(c.q.name,
		strconv.FormatInt(m.id, 10), m.body)

	err := c.c.writeProto(p.P)

	if err == nil && c.noAck {
		c.q.Ack(m.id)
	}

	return err
}

func (c *channel) Ack(msgId int64) {
	c.q.Ack(msgId)
}
