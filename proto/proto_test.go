package proto

import (
	"reflect"
	"testing"
)

func TestProto(t *testing.T) {
	p := new(Proto)

	p.Method = 100

	p.Fields = map[string]string{
		"key1": "value1",
	}

	p.Body = []byte("hello world")

	buf, err := Marshal(p)
	if err != nil {
		t.Fatal(err)
	}

	p2 := new(Proto)

	err = Unmarshal(buf, p2)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p, p2) {
		t.Fatal("not equal")
	}
}
