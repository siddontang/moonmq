package broker

import (
	"reflect"
	"testing"
)

func TestMsg(t *testing.T) {
	m := newMsg(0, 0, "abc", []byte("hello world"))

	buf, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}

	m2 := new(msg)

	if err := m2.Decode(buf); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(m, m2) {
		t.Fatal("not equal")
	}
}

func TestMsgStore(t *testing.T) {
	app := getTestApp()
	s := app.ms

	id, err := s.GenerateID()
	if err != nil {
		t.Fatal(err)
	}

	m := newMsg(id, 0, "abc", []byte("hello world"))

	if err := s.Save("test_queue", m); err != nil {
		t.Fatal(err)
	}

	if m1, err := s.Front("test_queue"); err != nil {
		t.Fatal(err)
	} else {
		if !reflect.DeepEqual(m, m1) {
			t.Fatal("not equal")
		}
	}

	if err := s.Delete("test_queue", m.id); err != nil {
		t.Fatal(err)
	}

	if m, err = s.Front("test_queue"); err != nil {
		t.Fatal(err)
	} else if m != nil {
		t.Fatal("delete failed")
	}
}
