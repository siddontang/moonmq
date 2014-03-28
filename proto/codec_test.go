package proto

import (
	"testing"

	"bytes"

	"reflect"
)

func TestCodec(t *testing.T) {
	p := NewProto()

	p.Method = 100
	p.Fields["Queue"] = "abc"

	wb := bytes.NewBuffer(nil)
	e := NewEncoder(wb)

	if err := e.Encode(p); err != nil {
		t.Fatal(err)
	}

	rb := bytes.NewBuffer(wb.Bytes())
	d := NewDecoder(rb)

	p2 := NewProto()

	if err := d.Decode(p2); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p, p2) {
		t.Fatal("error")
	}
}
