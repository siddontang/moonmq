package broker

import (
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strconv"
	"strings"
)

func (c *conn) handlePublish(p *proto.Proto) error {
	tp := p.PubType()
	queue := p.Queue()
	routingKey := p.RoutingKey()

	message := p.Body

	if len(message) == 0 {
		return c.protoError(http.StatusForbidden, "publish empty data forbidden")
	}

	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	t, ok := proto.PublishTypeMap[strings.ToLower(tp)]
	if !ok {
		return c.protoError(http.StatusBadRequest,
			fmt.Sprintf("invalid publish type %s", tp))
	}

	if c.app.cfg.MaxQueueSize > 0 {
		if n, err := c.app.ms.Len(queue, routingKey); err != nil {
			return c.protoError(http.StatusInternalServerError, err.Error())
		} else if n >= c.app.cfg.MaxQueueSize {
			if err = c.app.ms.Pop(queue, routingKey); err != nil {
				return c.protoError(http.StatusInternalServerError, err.Error())
			}
		}
	}

	id, err := c.app.ms.GenerateID()
	if err != nil {
		return c.protoError(http.StatusInternalServerError, "gen msgid error")
	}

	msg := newMsg(id, t, message)

	if err := c.app.ms.Save(queue, routingKey, msg); err != nil {
		return c.protoError(http.StatusInternalServerError, "save message error")
	}

	q := c.app.qs.Get(queue, routingKey)
	q.Push(msg)

	np := proto.NewPublishOKProto(strconv.FormatInt(id, 10))

	c.writeProto(np.P)

	return nil
}

func (c *conn) handleAck(p *proto.Proto) error {
	queue := p.Queue()

	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	if _, ok := c.routes[queue]; !ok {
		return c.protoError(http.StatusForbidden, "invalid queue")
	}

	routingKey := p.RoutingKey()

	msgId, err := strconv.ParseInt(p.MsgId(), 10, 64)
	if err != nil {
		return err
	}

	q := c.app.qs.Getx(queue, routingKey)
	if q == nil {
		return c.protoError(http.StatusBadRequest, "invalid ack fields")
	}

	q.Ack(msgId)

	return nil
}

func (c *conn) Push(queue string, routingKey string, m *msg) error {
	noAck := c.HasNoAck(queue)

	p := proto.NewPushProto(queue, routingKey,
		strconv.FormatInt(m.id, 10), m.body, noAck)

	err := c.writeProto(p.P)

	if err == nil && noAck {
		q := c.app.qs.Getx(queue, routingKey)
		q.Ack(m.id)
	}

	return err
}

func (c *conn) HasNoAck(queue string) bool {
	if _, ok := c.noAcks[queue]; ok {
		return true
	} else {
		return false
	}
}
