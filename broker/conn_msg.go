package broker

import (
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strconv"
	"strings"
)

func checkPublish(queue string, routingKey string, tp string, message []byte) error {
	if len(message) == 0 {
		return fmt.Errorf("publish empty data forbidden")
	} else if len(queue) == 0 {
		return fmt.Errorf("queue empty forbidden")
	} else if len(queue) > proto.MaxQueueName {
		return fmt.Errorf("queue too long")
	} else if len(routingKey) > proto.MaxRoutingKeyName {
		return fmt.Errorf("routingkey too long")
	}

	_, ok := proto.PublishTypeMap[strings.ToLower(tp)]
	if !ok {
		return fmt.Errorf("invalid publish type %s", tp)
	}

	return nil
}

func (app *App) saveMsg(queue string, routingKey string, tp string, message []byte) (*msg, error) {
	t, _ := proto.PublishTypeMap[strings.ToLower(tp)]

	if app.cfg.MaxQueueSize > 0 {
		if n, err := app.ms.Len(queue); err != nil {
			return nil, err
		} else if n >= app.cfg.MaxQueueSize {
			if err = app.ms.Pop(queue); err != nil {
				return nil, err
			}
		}
	}

	id, err := app.ms.GenerateID()
	if err != nil {
		return nil, err
	}

	msg := newMsg(id, t, routingKey, message)

	if err := app.ms.Save(queue, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (c *conn) handlePublish(p *proto.Proto) error {
	tp := p.PubType()
	queue := p.Queue()
	routingKey := p.RoutingKey()

	message := p.Body

	if err := checkPublish(queue, routingKey, tp, message); err != nil {
		return c.protoError(http.StatusBadRequest, err.Error())
	}

	msg, err := c.app.saveMsg(queue, routingKey, tp, message)
	if err != nil {
		return c.protoError(http.StatusInternalServerError, err.Error())
	}

	q := c.app.qs.Get(queue)
	q.Push(msg)

	np := proto.NewPublishOKProto(strconv.FormatInt(msg.id, 10))

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
