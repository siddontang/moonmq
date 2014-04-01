package proto

import (
	"bufio"
	"encoding/binary"
	"io"
)

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	e := new(Encoder)

	e.w = w

	return e
}

func (e *Encoder) Encode(p *Proto) error {
	if buf, err := Marshal(p); err != nil {
		return err
	} else {
		_, err = e.w.Write(buf)
		return err
	}
}

type Decoder struct {
	r *bufio.Reader
}

const defaultReaderSize = 128

func NewDecoder(r io.Reader) *Decoder {
	d := new(Decoder)
	if v, ok := r.(*bufio.Reader); ok {
		d.r = v
	} else {
		d.r = bufio.NewReaderSize(r, defaultReaderSize)
	}

	return d
}

func (d *Decoder) Decode(p *Proto) error {
	buf, err := d.r.Peek(4)
	if err != nil {
		return err
	}

	lenght := binary.BigEndian.Uint32(buf)
	buf = make([]byte, lenght+4)

	if _, err := io.ReadFull(d.r, buf); err != nil {
		return err
	}

	err = Unmarshal(buf, p)
	return err
}

func (d *Decoder) DecodeProto() (*Proto, error) {
	var p Proto
	err := d.Decode(&p)

	return &p, err
}
