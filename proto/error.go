package proto

import (
	"strconv"
)

// Method: Error
// Fields:
//     code: xxx (http error code, int string)
// Body: message
type ProtoError struct {
	P *Proto
}

func NewProtoError(code int, message string) *ProtoError {
	p := new(ProtoError)
	p.P = NewProto(Error,
		map[string]string{
			CodeStr: strconv.Itoa(code),
		},
		[]byte(message))

	return p
}

func (p *ProtoError) Error() string {
	return string(p.P.Body)
}
