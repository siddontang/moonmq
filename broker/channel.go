package broker

import (
	"github.com/siddontang/moonmq/proto"
)

//if a conn bind a queue, we represent it as a channel
type channel struct {
	c *conn
	q *queue

	routingKeys []string
}

func newChannel(c *conn, q *queue, keys []string) *channel {
	ch := new(channel)
	ch.c = c
	ch.q = q

	ch.routingKeys = keys

	return ch
}

func (c *channel) Push(m *msg) error {
	p := proto.NewProto(proto.Push, map[string]string{
		proto.QueueStr: c.q.name,
	}, m.body)

	return c.c.writeProto(p)
}
