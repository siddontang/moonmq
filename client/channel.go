package client

import (
	"errors"
	"time"
)

var ErrChannelClosed = errors.New("channel has been closed")

type channelMsg struct {
	ID   string
	Body []byte
}

type Channel struct {
	c          *Conn
	queue      string
	routingKey string
	noAck      bool

	msg    chan *channelMsg
	closed bool

	lastId string
}

func newChannel(c *Conn, queue string, routingKey string, noAck bool) *Channel {
	ch := new(Channel)

	ch.c = c
	ch.queue = queue
	ch.routingKey = routingKey
	ch.noAck = noAck

	ch.msg = make(chan *channelMsg, c.cfg.MaxQueueSize)

	ch.closed = false
	return ch
}

func (c *Channel) Close() error {
	c.closed = true

	return c.c.unbind(c.queue)
}

func (c *Channel) Ack() error {
	if c.closed {
		return ErrChannelClosed
	}

	return c.c.ack(c.queue, c.lastId)
}

func (c *Channel) GetMsg() []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	msg := <-c.msg
	c.lastId = msg.ID
	return msg.Body
}

func (c *Channel) WaitMsg(d time.Duration) []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	select {
	case <-time.After(d):
		return nil
	case msg := <-c.msg:
		c.lastId = msg.ID
		return msg.Body
	}
}

func (c *Channel) pushMsg(msgId string, body []byte) {
	for {
		select {
		case c.msg <- &channelMsg{msgId, body}:
			return
		default:
			<-c.msg
		}
	}
}
