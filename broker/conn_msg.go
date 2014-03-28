package broker

import (
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"net/http"
	"strings"
)

func (c *conn) handlePublish(p *proto.Proto) error {
	tp := p.Fields["Type"]
	queue := p.Fields["Queue"]
	routingKey := p.Fields["Routing-Key"]
	message := p.Body

	if len(message) == 0 {
		return c.protoError(http.StatusForbidden, "publish empty data forbidden")
	}

	switch v := strings.ToLower(tp); {
	case "direct":
	case "fanout":
	default:
		return c.protoError(http.StatusBadRequest,
			fmt.Sprintf("invalid publish type %s", v))
	}

	return nil
}

func (c *conn) handleAck(p *proto.Proto) error {
	return nil
}
