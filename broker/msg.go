package broker

import (
	"encoding/binary"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type msgMeta struct {
	id         int64
	ctime      int64
	pubType    uint8
	routingKey string
}
type msgData struct {
	id   int64
	body []byte
}

func (m *msgMeta) Encode() ([]byte, error) {
	//max routing key is 128, we can use 1 byte to repr length
	length := 10 + len(m.routingKey)
	buf := make([]byte, length+2)

	pos := 0
	binary.LittleEndian.PutUint16(buf[pos:], uint16(length))
	pos += 2

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.ctime))
	pos += 8

	buf[pos] = uint8(m.pubType)
	pos += 1

	buf[pos] = uint8(len(m.routingKey))
	pos += 1

	copy(buf[pos:], m.routingKey)

	return buf, nil
}

func (m *msgMeta) Decode(buf []byte) error {
	if len(buf) < 12 {
		return fmt.Errorf("meta buf short")
	}

	pos := 0
	length := int(binary.LittleEndian.Uint16(buf[pos:]))
	if length != len(buf)-2 {
		return fmt.Errorf("invalid meta buf len")
	}
	pos += 2

	m.ctime = int64(binary.LittleEndian.Uint64(buf[pos:]))
	pos += 8

	m.pubType = uint8(buf[pos])
	pos += 1

	routingKeyLen := int(uint8(buf[pos]))
	pos += 1

	m.routingKey = string(buf[pos : pos+routingKeyLen])
	pos += routingKeyLen

	return nil
}

type msg struct {
	id         int64
	ctime      int64
	pubType    uint8
	routingKey string

	body []byte
}

func newMsg(id int64, pubType uint8,
	routingKey string, body []byte) *msg {
	m := new(msg)

	m.body = body

	m.id = id

	m.ctime = time.Now().Unix()

	m.pubType = pubType
	m.routingKey = routingKey

	return m
}

func (m *msg) Meta() *msgMeta {
	meta := new(msgMeta)
	meta.id = m.id
	meta.pubType = m.pubType
	meta.routingKey = m.routingKey
	meta.ctime = m.ctime
	return meta
}

func (m *msg) Data() *msgData {
	d := new(msgData)
	d.id = m.id
	d.body = m.body
	return d
}

type msgStore struct {
	keyPrefix string
	app       *App
	redis     *redis.Pool
}

func newMsgStore(app *App) *msgStore {
	s := new(msgStore)

	s.app = app
	s.redis = app.redis

	s.keyPrefix = app.cfg.KeyPrefix

	return s
}

func (s *msgStore) GenerateID() (int64, error) {
	key := fmt.Sprintf("%s:base:msg_id", s.keyPrefix)
	c := s.redis.Get()
	n, err := redis.Int64(c.Do("INCR", key))
	c.Close()

	return n, err
}

func (s *msgStore) metaKey(queue string) string {
	return fmt.Sprint("%s:queue:%s:meta", s.keyPrefix, queue)
}

func (s *msgStore) dataKey(queue string) string {
	return fmt.Sprint("%s:queue:%s:data", s.keyPrefix, queue)
}

func (s *msgStore) Save(queue string, m *msg) error {
	meta := m.Meta()

	buf, err := meta.Encode()
	if err != nil {
		return err
	}

	metaKey := s.metaKey(queue)

	dataKey := s.dataKey(queue)

	c := s.redis.Get()

	_, err = c.Do("ZADD", dataKey, m.id, m.body)
	if err != nil {
		c.Close()
		return err
	}

	_, err = c.Do("ZADD", metaKey, m.id, buf)
	c.Close()

	return err
}

func (s *msgStore) handleRangeData(reply interface{}, err error) ([]*msgData, error) {
	var values []interface{}

	values, err = redis.Values(reply, err)
	if err != nil || err != redis.ErrNil {
		return nil, err
	} else if err == redis.ErrNil {
		return []*msgData{}, nil
	}

	if len(values)%2 != 0 {
		return nil, fmt.Errorf("invalid range withscores")
	}

	datas := make([]*msgData, 0, len(values)/2)

	for i := 0; i < len(values); i = i + 2 {
		data := new(msgData)
		data.id, err = redis.Int64(values[i], nil)
		if err != nil {
			continue
		}
		data.body, err = redis.Bytes(values[i], nil)
		if err != nil {
			continue
		}

		datas = append(datas, data)
	}

	return datas, nil
}

func (s *msgStore) handleRangeMeta(reply interface{}, err error) ([]*msgMeta, error) {
	var datas []*msgData
	datas, err = s.handleRangeData(reply, err)
	if err != nil {
		return nil, err
	}

	metas := make([]*msgMeta, 0, len(datas))
	for _, v := range datas {
		m := new(msgMeta)
		m.id = v.id

		if err := m.Decode(v.body); err != nil {
			continue
		}

		metas = append(metas, m)
	}

	return metas, nil
}

func (s *msgStore) RangeMeta(queue string, start int, stop int) ([]*msgMeta, error) {
	key := s.metaKey(queue)

	c := s.redis.Get()

	reply, err := c.Do("ZRANGE", key, start, stop, "WITHSCORES")

	c.Close()

	return s.handleRangeMeta(reply, err)
}

func (s *msgStore) RangeData(queue string, start int, stop int) ([]*msgData, error) {
	key := s.dataKey(queue)

	c := s.redis.Get()

	reply, err := c.Do("ZRANGE", key, start, stop, "WITHSCORES")

	c.Close()

	return s.handleRangeData(reply, err)

	return nil, nil
}

func (s *msgStore) RangeMetaByID(queue string, minID int64, maxID int64) ([]*msgMeta, error) {
	key := s.metaKey(queue)

	c := s.redis.Get()

	reply, err := c.Do("ZRANGEBYSCORE", key, minID, maxID, "WITHSCORES")

	c.Close()

	return s.handleRangeMeta(reply, err)
}

func (s *msgStore) RangeDataByID(queue string, minID int64, maxID int64) ([]*msgData, error) {
	key := s.dataKey(queue)

	c := s.redis.Get()

	reply, err := c.Do("ZRANGEBYSCORE", key, minID, maxID, "WITHSCORES")

	c.Close()

	return s.handleRangeData(reply, err)
}

func (s *msgStore) RemRange(queue string, start int, stop int) error {
	metaKey := s.metaKey(queue)
	dataKey := s.dataKey(queue)

	c := s.redis.Get()

	_, err := c.Do("ZREMRANGEBYRANK", metaKey, start, stop)

	_, err = c.Do("ZREMRANGEBYRANK", dataKey, start, stop)

	c.Close()

	return err
}

func (s *msgStore) RemRangeByID(queue string, minID int64, maxID int64) error {
	metaKey := s.metaKey(queue)
	dataKey := s.dataKey(queue)

	c := s.redis.Get()

	_, err := c.Do("ZREMRANGEBYSCORE", metaKey, minID, maxID)

	_, err = c.Do("ZREMRANGEBYSCORE", dataKey, minID, maxID)

	c.Close()

	return err
}
