package broker

import (
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strings"
)

func (c *conn) handleBind(p *proto.Proto) error {
	queue := p.Fields[proto.QueueStr]
	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	routingKeys := strings.Split(p.Fields[proto.RoutingKeyStr], ",")

	q := c.app.qs.Get(queue)

	ch, ok := c.chs[queue]
	if ok {
		q.Unbind(ch)
		ch.routingKeys = routingKeys
		q.Bind(ch)
	} else {
		ch := newChannel(c, q, routingKeys)
		c.chs[queue] = ch

		q.Bind(ch)
	}

	np := proto.NewProto(proto.Bind_OK, map[string]string{
		proto.QueueStr: queue,
	}, nil)

	c.writeProto(np)

	return nil
}

func (c *conn) handleUnbind(p *proto.Proto) error {
	queue := p.Fields[proto.QueueStr]
	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	q := c.app.qs.Get(queue)

	ch, ok := c.chs[queue]
	if !ok {
		return c.protoError(http.StatusForbidden, "queue not bind before")
	}

	q.Unbind(ch)

	np := proto.NewProto(proto.Unbind_OK, map[string]string{
		proto.QueueStr: queue,
	}, nil)

	c.writeProto(np)

	return nil
}
