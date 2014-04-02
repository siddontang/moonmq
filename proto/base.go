package proto

// Method: Heartbeat
// Fields: nil
// Body: nil
type HeartbeatProto struct {
	P *Proto
}

func NewHeartbeatProto() *HeartbeatProto {
	var p HeartbeatProto

	p.P = NewProto(Heartbeat, nil, nil)

	return &p
}
