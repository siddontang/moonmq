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

	noAck := (p.Fields[proto.NoAckStr] == "1")

	if noAck {
		c.noAcks[queue] = struct{}{}
	}

	routingKeys := strings.Split(p.Fields[proto.RoutingKeyStr], ",")

	rqs, ok := c.routes[queue]
	if ok {
		for _, routingKey := range rqs {
			rq := c.app.qs.Getx(queue, routingKey)
			if rq != nil {
				rq.Unbind(c)
			}
		}
	}

	c.routes[queue] = routingKeys

	for _, routingKey := range routingKeys {
		rq := c.app.qs.Get(queue, routingKey)
		rq.Bind(c)
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

	delete(c.noAcks, queue)

	rqs, ok := c.routes[queue]
	if ok {
		for _, routingKey := range rqs {
			rq := c.app.qs.Getx(queue, routingKey)
			if rq != nil {
				rq.Unbind(c)
			}
		}
	}

	np := proto.NewProto(proto.Unbind_OK, map[string]string{
		proto.QueueStr: queue,
	}, nil)

	c.writeProto(np)

	return nil
}
