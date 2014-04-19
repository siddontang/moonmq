package broker

import (
	"encoding/json"
	"fmt"
)

type StoreDriver interface {
	Open(configJson json.RawMessage) (Store, error)
}

type Store interface {
	Close() error
	GenerateID() (int64, error)
	Save(queue string, m *msg) error
	Delete(queue string, msgId int64) error
	Pop(queue string) error
	Front(queue string) (*msg, error)
	Len(queue string) (int, error)
}

var stores = map[string]StoreDriver{}

func RegisterStore(name string, d StoreDriver) error {
	if _, ok := stores[name]; ok {
		return fmt.Errorf("%s has been registered", name)
	}

	stores[name] = d
	return nil
}

func OpenStore(name string, configJson json.RawMessage) (Store, error) {
	d, ok := stores[name]
	if !ok {
		return nil, fmt.Errorf("%s has not been registered", name)
	}

	return d.Open(configJson)
}
