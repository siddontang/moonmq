package broker

// import (
// 	"encoding/binary"
// 	"encoding/json"
// 	"fmt"
// 	"github.com/siddontang/go-leveldb/leveldb"
// 	"github.com/siddontang/golib/hack"
// 	"sync/atomic"
// )

// type LevelDBConfig struct {
// 	Path string `json:"path"`

// 	Compression bool `json:"compression"`

// 	BlockSize       int `json:"block_size"`
// 	WriteBufferSize int `json:"write_buffer_size"`
// 	CacheSize       int `json:"cache_size"`

// 	KeyPrefix string `json:"key_prefix"`
// }

// type LevelDBStoreDriver struct {
// }

// func (d LevelDBStoreDriver) Open(jsonConfig json.RawMessage) (Store, error) {
// 	return newLevelDBStore(jsonConfig)
// }

// type LevelDBStore struct {
// 	cfg *LevelDBConfig

// 	db *leveldb.DB

// 	msgId int64

// 	keyPrefix string

// 	msgIdKey []byte
// }

// func newLevelDBStore(jsonConfig json.RawMessage) (*LevelDBStore, error) {
// 	s := new(LevelDBStore)
// 	cfg := new(LevelDBConfig)

// 	s.cfg = cfg

// 	s.keyPrefix = cfg.KeyPrefix

// 	var err error

// 	if err = json.Unmarshal(jsonConfig, cfg); err != nil {
// 		return nil, err
// 	}

// 	s.db, err = leveldb.Open(jsonConfig)
// 	if err != nil {
// 		return nil, err
// 	}

// 	s.msgIdKey = []byte(fmt.Sprintf("%s:base:msg_id", s.keyPrefix))

// 	if v, err := s.db.Get(s.msgIdKey); err != nil {
// 		return nil, err
// 	} else if v != nil {
// 		s.msgId = int64(binary.LittleEndian.Uint64(v))
// 	} else {
// 		s.msgId = 0
// 	}

// 	return s, nil
// }

// func (s *LevelDBStore) key(queue string, id int64) []byte {
// 	k := fmt.Sprintf("%s:queue:%s:%016x", s.keyPrefix, queue, id)
// 	return hack.Slice(k)
// }

// func (s *LevelDBStore) Close() error {
// 	s.db.Close()
// 	s.db = nil
// 	return nil
// }

// func (s *LevelDBStore) GenerateID() (int64, error) {
// 	id := atomic.AddInt64(&s.msgId, 1)

// 	v := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(v, uint64(id))

// 	if err := s.db.Put(s.msgIdKey, v); err != nil {
// 		return 0, err
// 	}

// 	return id, nil
// }

// //leveldb will save infinite msgs
// func (s *LevelDBStore) Len(queue string) (int, error) {
// 	return 0, nil
// }

// func (s *LevelDBStore) Save(queue string, m *msg) error {
// 	key := s.key(queue, m.id)

// 	buf, _ := m.Encode()

// 	return s.db.Put(key, buf)
// }

// func (s *LevelDBStore) Delete(queue string, msgId int64) error {
// 	key := s.key(queue, msgId)

// 	return s.db.Delete(key)
// }

// func (s *LevelDBStore) Pop(queue string) error {
// 	key := s.key(queue, 0)

// 	it := s.db.Iterator(key, nil, 1)
// 	defer it.Close()

// 	if it.Valid() {
// 		return s.db.Delete(it.Key())
// 	} else {
// 		return nil
// 	}
// }

// func (s *LevelDBStore) Front(queue string) (*msg, error) {
// 	key := s.key(queue, 0)

// 	it := s.db.Iterator(key, nil, 1)
// 	defer it.Close()

// 	if it.Valid() {
// 		buf := it.Value()
// 		m := new(msg)
// 		if err := m.Decode(buf); err != nil {
// 			return nil, err
// 		} else {
// 			return m, nil
// 		}
// 	}

// 	return nil, nil
// }

// func init() {
// 	RegisterStore("leveldb", LevelDBStoreDriver{})
// }
