package broker

import (
	"github.com/siddontang/moonmq/proto"
	"net"
	"runtime"
	"time"
)

type conn struct {
	app *App

	c net.Conn

	decoder *proto.Decoder

	wc chan []byte

	quit chan struct{}

	lastUpdate int64
}

func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.decoder = proto.NewDecoder(co)

	c.wc = make(chan []byte, 4)

	c.quit = make(chan struct{})

	return c
}

func (c *conn) run() {
	go c.onWrite()

	c.onRead()
}

func (c *conn) onRead() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			c.app.errLog.Fatal("crash %v:%v", err, buf)
		}

		c.c.Close()
		close(c.quit)
	}()

	for {
		p, err := c.readProto()
		if err != nil {
			c.app.errLog.Info("on read error %v", err)
			return
		}

		switch p.Method {
		case proto.Publish:
			err = c.handlePublish(p)
		case proto.Bind:
		case proto.Unbind:
		case proto.Ack:
			err = c.handleAck(p)
		case proto.Heartbeat:
			c.lastUpdate = time.Now().Unix()
		default:
			c.app.errLog.Info("invalid proto method %d", p.Method)
			return
		}

		if err != nil {
			c.writeError(err)
		}
	}
}

func (c *conn) readProto() (*proto.Proto, error) {
	p := proto.NewProto()

	err := c.decoder.Decode(p)

	return p, err
}

func (c *conn) onWrite() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			c.app.errLog.Fatal("crash %v:%v", err, buf)
		}
	}()

	for {
		select {
		case buf := <-c.wc:
			if n, err := c.c.Write(buf); err != nil {
				c.app.errLog.Info("write error %v", err)
				c.c.Close()
			} else if n != len(buf) {
				c.app.errLog.Info("write not complete %d != %d", n, len(buf))
				c.c.Close()
			}
		case <-c.app.wheel.After(time.Duration(c.app.cfg.KeepAlive)):
			if time.Now().Unix()-c.lastUpdate > int64(1.5*float32(c.app.cfg.KeepAlive)) {
				c.app.errLog.Info("keepalive timeout")
				c.c.Close()
			}
		case <-c.quit:
			return
		}
	}
}

func (c *conn) writeError(err error) {
	var p *proto.Proto
	if pe, ok := err.(*proto.ProtoError); ok {
		p = pe.P
	} else {
		p = proto.NewProto()
		p.Fields["Code"] = "500"
		p.Body = []byte(err.Error())
	}

	p.Version = c.app.cfg.Version
	p.Method = proto.Error
	c.writeProto(p)
}

func (c *conn) protoError(code int, message string) error {
	return proto.NewProtoError(code, message)
}

func (c *conn) writeProto(p *proto.Proto) {
	if buf, err := p.Encode(); err != nil {
		c.app.errLog.Error("proto encode error %v", err)
	} else {
		c.wc <- buf
	}
}
