package proto

import (
	"testing"

	"bytes"

	"reflect"
)

func TestCodec(t *testing.T) {
	p := NewProto(100, map[string]string{"Queue": "abc"}, nil)

	wb := bytes.NewBuffer(nil)
	e := NewEncoder(wb)

	if err := e.Encode(p); err != nil {
		t.Fatal(err)
	}

	rb := bytes.NewBuffer(wb.Bytes())
	d := NewDecoder(rb)

	var p2 *Proto

	var err error
	if p2, err = d.Decode(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p, p2) {
		t.Fatal("error")
	}
}
