package proto

// Refer ampq protocol, we have below methods:

// 1, synchronous request, must wait for the special reply method,
//  but can handle asynchronous method when waits
// 2, synchronous reply, to a special synchronous request
// 3, asynchronous request or reply

// synchronous request is even number
// synchronous reply is odd number

// asynchronous is even number

// handshake proto

// Method: Handshake
// Fields: nil
// Body: nil

// handshake_ok proto

// Method: Handshake_OK
// Fields:
// Body: nil

// publish proto

// Method: Publish
// Fields:
//   //direct|fanout
//   //direct select a consumer to push using round-robin
//   //fanout broadcast to all consumers
//   Type = xxx

//   //Queue to store message
//   Queue = xxx

//   //publish to sepcial consumers binding routing-key
//   Routing-Key = xxx

// Body: publish data

// publish_ok proto

// Method: Publish_OK

// Fields: nil

// Body:
//  msg id ((bigendian int64))

// ack proto

// Method: Ack

// Fields:
//   Queue = xxx
// Body:
// msg id (bigendian int64)

// error proto

// Method: Error

// Fields:
//   //error code, int string, use http error code
//   Code = xxx

// Body: text base error msg

// heartbeat proto

// Method: Heartbeat

// Fields: nil
// Body: nil

// bind proto

// Method: Bind
// Fields:
//     //Queue to get message
//     Queue = xxx

//     //bind sepcial routing key, seperated by comma
//     Routing-Key = xxx,xxx,xxx

// Body: nil

// bind_ok proto

// Method: Bind_OK

// Fields:
//      Queue = xxx
// Body: nil

// unbind proto

// Method: Unbind

// Fields:
//      Queue = xxx

// Body: nil

// unbind_ok proto

// Method: Unbind_OK

// Fields:
//      Queue = xxx
// Body: nil

// push proto

// Method: Push

// Fields:
//      Queue = xxx

// Body:
//  publish data
