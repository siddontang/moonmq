package broker

import (
	"encoding/json"
	"fmt"
	"sync"
)

type MemStoreDriver struct {
}

func (d MemStoreDriver) Open(jsonConfig json.RawMessage) (Store, error) {
	return newMemStore()
}

type MemStore struct {
	sync.Mutex

	msgID int64

	msgs map[string][]*msg
}

func newMemStore() (*MemStore, error) {
	s := new(MemStore)

	s.msgID = 0
	s.msgs = make(map[string][]*msg)

	return s, nil
}

func (s *MemStore) GenerateID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	s.msgID++
	return s.msgID, nil
}

func (s *MemStore) key(queue string) string {
	return fmt.Sprintf("%s", queue)
}

func (s *MemStore) Close() error {
	return nil
}

func (s *MemStore) Save(queue string, m *msg) error {
	key := s.key(queue)

	s.Lock()
	defer s.Unlock()

	q, ok := s.msgs[key]
	if !ok {
		q = make([]*msg, 0, 1)
	}

	s.msgs[key] = append(q, m)

	return nil
}

func (s *MemStore) Delete(queue string, msgId int64) error {
	key := s.key(queue)

	s.Lock()
	defer s.Unlock()

	q, ok := s.msgs[key]
	if !ok {
		return nil
	}

	for i, m := range q {
		if m.id == msgId {
			copy(q[i:], q[i+1:])
			q[len(q)-1] = nil

			q = q[:len(q)-1]
			if len(q) == 0 {
				delete(s.msgs, key)
			} else {
				s.msgs[key] = q
			}

			return nil
		}
	}

	return nil
}

func (s *MemStore) Pop(queue string) error {
	key := s.key(queue)

	s.Lock()
	defer s.Unlock()

	q, ok := s.msgs[key]
	if !ok {
		return nil
	}

	if len(q) == 0 {
		return nil
	}

	copy(q[0:], q[1:])
	q[len(q)-1] = nil
	s.msgs[key] = q[:len(q)-1]

	return nil
}

func (s *MemStore) Len(queue string) (int, error) {
	key := s.key(queue)

	s.Lock()
	defer s.Unlock()

	q, ok := s.msgs[key]
	if !ok {
		return 0, nil
	}

	return len(q), nil
}

func (s *MemStore) Front(queue string) (*msg, error) {
	key := s.key(queue)

	s.Lock()
	defer s.Unlock()

	q, ok := s.msgs[key]
	if !ok {
		return nil, nil
	}

	if len(q) == 0 {
		return nil, nil
	} else {
		return q[0], nil
	}
}

func init() {
	RegisterStore("mem", MemStoreDriver{})
}
