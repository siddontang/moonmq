package proto

// Method: Publish
// Fields:
//     queue: xxx
//     routing_key: xxx
//     //type: direct|fanout
//     //direct select a consumer to push using round-robin
//     //fanout broadcast to all consumers, ignore routing key
//     pub_type: xxx
// Body:
//     body
type PublishProto struct {
	P *Proto
}

func NewPublishProto(queue string, routingKey string, pubType string, body []byte) *PublishProto {
	var p PublishProto

	p.P = NewProto(Publish, map[string]string{
		QueueStr:      queue,
		RoutingKeyStr: routingKey,
		PubTypeStr:    pubType,
	}, body)

	return &p
}

// Method: Publish_OK
// Fields: nil
// Body: msg id (int64 string)
type PublishOKProto struct {
	P *Proto
}

func NewPublishOKProto(msgId string) *PublishOKProto {
	var p PublishOKProto

	p.P = NewProto(Publish_OK, nil, []byte(msgId))

	return &p
}

// Method: Push
// Fields:
//     queue: xxx
//     msg_id: xxx
// Body:
//     body
type PushProto struct {
	P *Proto
}

func NewPushProto(queue string, msgId string, body []byte) *PushProto {
	var p PushProto

	p.P = NewProto(Push, map[string]string{
		QueueStr: queue,
		MsgIdStr: msgId,
	}, body)

	return &p
}

// Method: Ack
// Fields:
//     queue: xxx
//     msg_id: xxx (int64 string)
type AckProto struct {
	P *Proto
}

func NewAckProto(queue string, msgId string) *AckProto {
	var p AckProto

	p.P = NewProto(Ack, map[string]string{
		QueueStr: queue,
		MsgIdStr: msgId,
	}, nil)

	return &p
}
