package broker

import (
	"encoding/binary"
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strings"
)

func (c *conn) handlePublish(p *proto.Proto) error {
	tp := p.Fields[proto.TypeStr]
	queue := p.Fields[proto.QueueStr]
	routingKey := p.Fields[proto.RoutingKeyStr]
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

	msgBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(msgBuf, uint64(id))

	np := proto.NewProto(proto.Publish_OK, nil, msgBuf)

	c.writeProto(np)

	return nil
}

func (c *conn) handleAck(p *proto.Proto) error {
	queue := p.Fields[proto.QueueStr]

	if len(queue) == 0 {
		return c.protoError(http.StatusForbidden, "queue must supplied")
	}

	if len(p.Body) != 8 {
		return c.protoError(http.StatusBadRequest, "invalid publish data")
	}

	msgId := int64(binary.BigEndian.Uint64(p.Body))

	q := c.app.qs.Get(queue)

	if _, ok := c.chs[queue]; !ok {
		return c.protoError(http.StatusForbidden, "unbind channel cannot ack")
	} else {
		q.Ack(msgId)
	}

	return nil
}
