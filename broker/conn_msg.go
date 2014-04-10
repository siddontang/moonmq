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
		return c.protoError(http.StatusBadRequest, "publish empty data forbidden")
	} else if len(queue) == 0 {
		return c.protoError(http.StatusBadRequest, "queue empty forbidden")
	} else if len(queue) > proto.MaxQueueName {
		return c.protoError(http.StatusBadRequest, "queue too long")
	} else if len(routingKey) > proto.MaxRoutingKeyName {
		return c.protoError(http.StatusBadRequest, "routingkey too long")
	}

	t, ok := proto.PublishTypeMap[strings.ToLower(tp)]
	if !ok {
		return c.protoError(http.StatusBadRequest,
			fmt.Sprintf("invalid publish type %s", tp))
	}

	if c.app.cfg.MaxQueueSize > 0 {
		if n, err := c.app.ms.Len(queue); err != nil {
			return c.protoError(http.StatusInternalServerError, err.Error())
		} else if n >= c.app.cfg.MaxQueueSize {
			if err = c.app.ms.Pop(queue); err != nil {
				return c.protoError(http.StatusInternalServerError, err.Error())
			}
		}
	}

	id, err := c.app.ms.GenerateID()
	if err != nil {
		return c.protoError(http.StatusInternalServerError, "gen msgid error")
	}

	msg := newMsg(id, t, routingKey, message)

	if err := c.app.ms.Save(queue, msg); err != nil {
		return c.protoError(http.StatusInternalServerError, "save message error")
	}

	q := c.app.qs.Get(queue)
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

	ch, ok := c.channels[queue]
	if !ok {
		return c.protoError(http.StatusForbidden, "invalid queue")
	}

	msgId, err := strconv.ParseInt(p.MsgId(), 10, 64)
	if err != nil {
		return err
	}

	ch.Ack(msgId)

	return nil
}
