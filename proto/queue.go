package proto

// Method: Bind
// Fields:
//     queue: xxx
//     routing_key: xxx
//     no_ack: 1 or none
// Body: nil
type BindProto struct {
	P *Proto
}

func NewBindProto(queue string, routingKey string, noAck bool) *BindProto {
	var p BindProto

	p.P = NewProto(Bind, map[string]string{
		QueueStr:      queue,
		RoutingKeyStr: routingKey,
	}, nil)

	if noAck {
		p.P.Fields[NoAckStr] = "1"
	}

	return &p
}

// Method: Bind_OK
// Fields:
//     queue: xxx
type BindOKProto struct {
	P *Proto
}

func NewBindOKProto(queue string) *BindOKProto {
	var p BindOKProto

	p.P = NewProto(Bind_OK, map[string]string{
		QueueStr: queue,
	}, nil)

	return &p
}

// Method: Unbind
// Fields:
//     queue: xxx
// Body: nil
type UnbindProto struct {
	P *Proto
}

func NewUnbindProto(queue string) *UnbindProto {
	var p UnbindProto

	p.P = NewProto(Unbind, map[string]string{
		QueueStr: queue,
	}, nil)

	return &p
}

// Method: Unbind_OK
// Fields:
//     //if queue is empty, we will unbind all queues
//     queue: xxx
type UnbindOKProto struct {
	P *Proto
}

func NewUnbindOKProto(queue string) *UnbindOKProto {
	var p UnbindOKProto

	p.P = NewProto(Unbind_OK, map[string]string{
		QueueStr: queue,
	}, nil)

	return &p
}
