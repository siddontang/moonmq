package broker

import (
	"encoding/binary"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type msg struct {
	id      int64
	ctime   int64
	pubType uint8
	body    []byte
}

func newMsg(id int64, pubType uint8, body []byte) *msg {
	m := new(msg)

	m.id = id
	m.ctime = time.Now().Unix()
	m.pubType = pubType
	m.body = body

	return m
}

func (m *msg) Encode() ([]byte, error) {
	lenBuf := 4 + 8 + 8 + 1 + len(m.body)
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

	copy(buf[pos:], m.body)
	return buf, nil
}

func (m *msg) Decode(buf []byte) error {
	if len(buf) < 13 {
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

	m.body = buf[pos:]

	return nil
}

type msgStore struct {
	app   *App
	redis *redis.Pool

	keyPrefix string
}

func newMsgStore(app *App) *msgStore {
	s := new(msgStore)

	s.app = app
	s.redis = app.redis

	s.keyPrefix = app.cfg.KeyPrefix

	return s
}

func (s *msgStore) key(queue string, routingKey string) string {
	return fmt.Sprintf("%s:queue:%s:%s", s.keyPrefix, queue, routingKey)
}

func (s *msgStore) GenerateID() (int64, error) {
	key := fmt.Sprintf("%s:base:msg_id", s.keyPrefix)
	c := s.redis.Get()
	n, err := redis.Int64(c.Do("INCR", key))
	c.Close()

	return n, err
}

func (s *msgStore) Save(queue string, routingKey string, m *msg) error {
	key := s.key(queue, routingKey)

	buf, _ := m.Encode()

	c := s.redis.Get()
	_, err := c.Do("ZADD", key, m.id, buf)
	c.Close()

	return err
}

func (s *msgStore) Delete(queue string, routingKey string, msgId int64) error {
	key := s.key(queue, routingKey)
	c := s.redis.Get()
	_, err := c.Do("ZREMRANGEBYSCORE", key, msgId, msgId)
	c.Close()

	return err
}

func (s *msgStore) Front(queue string, routingKey string) (*msg, error) {
	key := s.key(queue, routingKey)
	c := s.redis.Get()

	vs, err := redis.Values(c.Do("ZRANGE", key, 0, 0))
	c.Close()

	if err != nil && err != redis.ErrNil {
		return nil, err
	} else if err == redis.ErrNil {
		return nil, nil
	} else if len(vs) == 0 {
		return nil, nil
	} else if len(vs) > 1 {
		return nil, fmt.Errorf("front more than one msg")
	}

	buf := vs[0].([]byte)

	m := new(msg)
	if err = m.Decode(buf); err != nil {
		return nil, err
	}

	return m, nil
}
