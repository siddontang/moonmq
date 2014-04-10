package broker

import (
	"github.com/siddontang/moonmq/proto"
	"net/http"
)

func (c *conn) handleBind(p *proto.Proto) error {
	queue := p.Queue()
	routingKey := p.RoutingKey()

	if len(queue) == 0 {
		return c.protoError(http.StatusBadRequest, "queue empty forbidden")
	} else if len(queue) > proto.MaxQueueName {
		return c.protoError(http.StatusBadRequest, "queue too long")
	} else if len(routingKey) > proto.MaxRoutingKeyName {
		return c.protoError(http.StatusBadRequest, "routingkey too long")
	}

	noAck := (p.Value(proto.NoAckStr) == "1")

	ch, ok := c.channels[queue]
	if !ok {
		q := c.app.qs.Getx(queue)
		ch = newChannel(c, q, routingKey, noAck)
		c.channels[queue] = ch
	} else {
		ch.Reset(routingKey, noAck)
	}

	np := proto.NewBindOKProto(queue)

	c.writeProto(np.P)

	return nil
}

func (c *conn) handleUnbind(p *proto.Proto) error {
	queue := p.Queue()
	if len(queue) == 0 {
		c.unBindAll()

		np := proto.NewUnbindOKProto(queue)

		c.writeProto(np.P)
		return nil
	}

	if ch, ok := c.channels[queue]; ok {
		delete(c.channels, queue)
		ch.Close()
	}

	np := proto.NewUnbindOKProto(queue)

	c.writeProto(np.P)

	return nil
}
