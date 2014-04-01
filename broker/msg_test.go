package broker

import (
	"reflect"
	"testing"
)

func TestMsg(t *testing.T) {
	m := new(msgMeta)

	m.id = 1
	m.ctime = 0
	m.pubType = 0
	m.routingKey = "key"

	buf, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}

	m2 := new(msgMeta)

	m2.id = 1

	err = m2.Decode(buf)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(m, m2) {
		t.Fatal("not equal")
	}
}
