package proto

// Refer ampq protocol, we have below methods:

// 1, synchronous request, must wait for the special reply method,
//  but can handle asynchronous method when waits
// 2, synchronous reply, to a special synchronous request
// 3, asynchronous request or reply

// synchronous request is even number
// synchronous reply is odd number

// asynchronous is even number

const (
	Auth    uint32 = 0
	Auth_OK uint32 = 1

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
	MsgIdStr      = "Msg-Id"
	VersionStr    = "Version"
	TypeStr       = "Type"
	QueueStr      = "Queue"
	RoutingKeyStr = "Routing-Key"
	NoAckStr      = "No-Ack"
	CodeStr       = "Code"
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
