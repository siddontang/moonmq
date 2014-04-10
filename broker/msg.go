package broker

import (
	"encoding/binary"
	"fmt"
	"time"
)

type msg struct {
	id         int64
	ctime      int64
	pubType    uint8
	routingKey string
	body       []byte
}

func newMsg(id int64, pubType uint8, routingKey string, body []byte) *msg {
	m := new(msg)

	m.id = id
	m.ctime = time.Now().Unix()
	m.pubType = pubType
	m.routingKey = routingKey
	m.body = body

	return m
}

func (m *msg) Encode() ([]byte, error) {
	lenBuf := 4 + 8 + 8 + 1 + 1 + len(m.routingKey) + len(m.body)
	buf := make([]byte, lenBuf)

	pos := 0

	binary.BigEndian.PutUint32(buf[pos:], uint32(lenBuf))
	pos += 4

	binary.BigEndian.PutUint64(buf[pos:], uint64(m.id))
	pos += 8

	binary.BigEndian.PutUint64(buf[pos:], uint64(m.ctime))
	pos += 8

	buf[pos] = byte(m.pubType)
	pos++

	buf[pos] = byte(len(m.routingKey))
	pos++

	copy(buf[pos:], m.routingKey)
	pos += len(m.routingKey)

	copy(buf[pos:], m.body)
	return buf, nil
}

func (m *msg) Decode(buf []byte) error {
	if len(buf) < 4 {
		return fmt.Errorf("buf too short")
	}

	pos := 0
	lenBuf := int(binary.BigEndian.Uint32(buf[0:4]))
	if lenBuf != len(buf) {
		return fmt.Errorf("invalid buf len")
	}

	pos += 4

	m.id = int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
	pos += 8

	m.ctime = int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	m.pubType = uint8(buf[pos])
	pos++

	keyLen := int(uint8(buf[pos]))
	pos++
	m.routingKey = string(buf[pos : pos+keyLen])
	pos += keyLen

	m.body = buf[pos:]

	return nil
}
