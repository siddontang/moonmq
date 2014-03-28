package proto

import (
	"strconv"
)

type ProtoError struct {
	P *Proto
}

func NewProtoError(code int, message string) *ProtoError {
	p := new(ProtoError)
	p.P = NewProto()

	p.P.Method = Error

	p.P.Fields = map[string]string{
		"Code": strconv.Itoa(code),
	}

	p.P.Body = []byte(message)

	return p
}

func (p *ProtoError) Error() string {
	return string(p.P.Body)
}
