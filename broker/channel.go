package broker

type msgPusher interface {
	Push(ch *channel, m *msg) error
}

//use channel represent conn bind a queue

type channel struct {
	p          msgPusher
	q          *queue
	routingKey string
	noAck      bool
}

func newChannel(p msgPusher, q *queue, routingKey string, noAck bool) *channel {
	ch := new(channel)

	ch.p = p
	ch.q = q

	ch.routingKey = routingKey
	ch.noAck = noAck

	q.Bind(ch)

	return ch
}

func (c *channel) Reset(routingKey string, noAck bool) {
	c.routingKey = routingKey
	c.noAck = noAck
}

func (c *channel) Close() {
	c.q.Unbind(c)
}

func (c *channel) Push(m *msg) error {
	return c.p.Push(c, m)
}

func (c *channel) Ack(msgId int64) {
	c.q.Ack(msgId)
}
