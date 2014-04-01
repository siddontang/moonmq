package proto

const (
	Handshake    uint32 = 0
	Handshake_OK uint32 = 1

	Publish    uint32 = 10
	Publish_OK uint32 = 11

	Bind    uint32 = 20
	Bind_OK uint32 = 21

	Unbind    uint32 = 30
	Unbind_OK uint32 = 31

	//asynchronous > 10000
	Error     uint32 = 10010
	Heartbeat uint32 = 10020
	Push      uint32 = 10030
	Ack       uint32 = 10040
)

const (
	VersionStr    = "Version"
	TypeStr       = "Type"
	QueueStr      = "Queue"
	RoutingKeyStr = "Routing-Key"
	NoAckStr      = "No-Ack"
)

const (
	DirectType uint8 = 0
	FanoutType uint8 = 1
)

var PublishTypeMap = map[string]uint8{
	"direct": DirectType,
	"fanout": FanoutType,
}

const (
	MaxQueueName      = 128
	MaxRoutingKeyName = 128
)
