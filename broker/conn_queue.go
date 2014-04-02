package broker

import (
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strings"
)

func (c *conn) handleBind(p *proto.Proto) error {
	queue := p.Queue()
	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	noAck := (p.Value(proto.NoAckStr) == "1")

	if noAck {
		c.noAcks[queue] = struct{}{}
	}

	routingKeys := strings.Split(p.RoutingKey(), ",")

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

	np := proto.NewBindOKProto(queue)

	c.writeProto(np.P)

	return nil
}

func (c *conn) unbindAll() error {
	c.noAcks = map[string]struct{}{}

	for queue, rqs := range c.routes {
		for _, routingKey := range rqs {
			rq := c.app.qs.Getx(queue, routingKey)
			if rq != nil {
				rq.Unbind(c)
			}
		}
	}

	c.routes = map[string][]string{}

	np := proto.NewUnbindProto("")

	c.writeProto(np.P)

	return nil
}

func (c *conn) handleUnbind(p *proto.Proto) error {
	queue := p.Queue()
	if len(queue) == 0 {
		return c.unbindAll()
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

	np := proto.NewUnbindOKProto(queue)

	c.writeProto(np.P)

	return nil
}
