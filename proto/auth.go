package proto

// Method: Auth
// Fields: nil
// Body: md5sum
type AuthProto struct {
	P *Proto
}

func NewAuthProto(sum []byte) *AuthProto {
	var p AuthProto
	p.P = NewProto(Auth, nil, sum)
	return &p
}

type AuthOKProto struct {
	P *Proto
}

// Method: Auth_OK
// Fields: nil
// Body: nil
func NewAuthOKProto() *AuthOKProto {
	var p AuthOKProto

	p.P = NewProto(Auth_OK, nil, nil)

	return &p
}
