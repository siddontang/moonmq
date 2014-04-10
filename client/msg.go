package client

type Msg struct {
	ID    string
	Queue string
	Body  []byte
}
