package broker

import (
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strconv"
)

func checkBind(queue string, routingKey string) error {
	if len(queue) == 0 {
		return fmt.Errorf("queue empty forbidden")
	} else if len(queue) > proto.MaxQueueName {
		return fmt.Errorf("queue too long")
	} else if len(routingKey) > proto.MaxRoutingKeyName {
		return fmt.Errorf("routingkey too long")
	}
	return nil
}

type connMsgPusher struct {
	c *conn
}

func (p *connMsgPusher) Push(ch *channel, m *msg) error {
	po := proto.NewPushProto(ch.q.name,
		strconv.FormatInt(m.id, 10), m.body)

	err := p.c.writeProto(po.P)

	if err == nil && ch.noAck {
		ch.Ack(m.id)
	}

	return err
}

func (c *conn) handleBind(p *proto.Proto) error {
	queue := p.Queue()
	routingKey := p.RoutingKey()

	if err := checkBind(queue, routingKey); err != nil {
		return c.protoError(http.StatusBadRequest, err.Error())
	}

	noAck := (p.Value(proto.NoAckStr) == "1")

	ch, ok := c.channels[queue]
	if !ok {
		q := c.app.qs.Get(queue)
		ch = newChannel(&connMsgPusher{c}, q, routingKey, noAck)
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
