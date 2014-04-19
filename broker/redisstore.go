package broker

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
)

type RedisStoreConfig struct {
	Addr      string `json:"addr"`
	DB        int    `json:"db"`
	Password  string `json:"password"`
	IdleConns int    `json:"idle_conns"`
	KeyPrefix string `json:"key_prefix"`
}

type RedisStore struct {
	redis *redis.Pool

	cfg *RedisStoreConfig

	keyPrefix string
}

type RedisStoreDriver struct {
}

func (d RedisStoreDriver) Open(jsonConfig json.RawMessage) (Store, error) {
	return newRedisStore(jsonConfig)
}

func newRedisStore(jsonConfig json.RawMessage) (*RedisStore, error) {
	cfg := new(RedisStoreConfig)

	err := json.Unmarshal(jsonConfig, cfg)
	if err != nil {
		return nil, err
	}

	s := new(RedisStore)

	s.cfg = cfg
	s.keyPrefix = cfg.KeyPrefix

	f := func() (redis.Conn, error) {
		n := "tcp"
		if strings.Contains(cfg.Addr, "/") {
			n = "unix"
		}

		c, err := redis.Dial(n, cfg.Addr)
		if err != nil {
			return nil, err
		}

		if len(cfg.Password) > 0 {
			if _, err = c.Do("AUTH", cfg.Password); err != nil {
				c.Close()
				return nil, err
			}
		}

		if cfg.DB != 0 {
			if _, err = c.Do("SELECT", cfg.DB); err != nil {
				c.Close()
				return nil, err
			}
		}

		return c, nil
	}

	s.redis = redis.NewPool(f, cfg.IdleConns)
	return s, nil
}

func (s *RedisStore) key(queue string) string {
	return fmt.Sprintf("%s:queue:%s", s.keyPrefix, queue)
}

func (s *RedisStore) Close() error {
	s.redis.Close()
	s.redis = nil
	return nil
}

func (s *RedisStore) GenerateID() (int64, error) {
	key := fmt.Sprintf("%s:base:msg_id", s.keyPrefix)
	c := s.redis.Get()
	n, err := redis.Int64(c.Do("INCR", key))
	c.Close()

	return n, err
}

func (s *RedisStore) Save(queue string, m *msg) error {
	key := s.key(queue)

	buf, _ := m.Encode()

	c := s.redis.Get()
	_, err := c.Do("ZADD", key, m.id, buf)
	c.Close()

	return err
}

func (s *RedisStore) Delete(queue string, msgId int64) error {
	key := s.key(queue)
	c := s.redis.Get()
	_, err := c.Do("ZREMRANGEBYSCORE", key, msgId, msgId)
	c.Close()

	return err
}

func (s *RedisStore) Pop(queue string) error {
	key := s.key(queue)
	c := s.redis.Get()
	_, err := c.Do("ZREMRANGEBYRANK", key, 0, 0)
	c.Close()

	return err
}

func (s *RedisStore) Len(queue string) (int, error) {
	key := s.key(queue)
	c := s.redis.Get()
	n, err := redis.Int(c.Do("ZCOUNT", key, "-inf", "+inf"))
	c.Close()

	return n, err
}

func (s *RedisStore) Front(queue string) (*msg, error) {
	key := s.key(queue)
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

func init() {
	RegisterStore("redis", RedisStoreDriver{})
}
