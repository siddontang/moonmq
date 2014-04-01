package broker

import (
	"fmt"
	"github.com/siddontang/golib/log"
	"github.com/siddontang/moonmq/proto"
	"net"
	"runtime"
	"sync"
	"time"
)

type conn struct {
	sync.Mutex

	app *App

	c net.Conn

	decoder *proto.Decoder

	lastUpdate int64

	handshaked bool

	routes map[string][]string
}

func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.handshaked = false

	c.decoder = proto.NewDecoder(co)

	c.checkKeepAlive()

	c.routes = make(map[string][]string)

	return c
}

func (c *conn) run() {
	c.onRead()
}

func (c *conn) onRead() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			log.Fatal("crash %v:%v", err, buf)
		}

		c.c.Close()

	}()

	for {
		p, err := c.decoder.DecodeProto()
		if err != nil {
			log.Info("on read error %v", err)
			return
		}

		if p.Method == proto.Handshake {
			err = c.handleHandshake(p)
		} else {
			if !c.handshaked {
				err = fmt.Errorf("must handshake first")
			} else {
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
					log.Info("invalid proto method %d", p.Method)
					return
				}
			}
		}

		if err != nil {
			c.writeError(err)
		}
	}
}

func (c *conn) handleHandshake(p *proto.Proto) error {
	//later check authorization

	rp := proto.NewProto(proto.Handshake_OK, nil, nil)
	c.writeProto(rp)

	c.handshaked = true

	return nil
}

func (c *conn) writeError(err error) {
	var p *proto.Proto
	if pe, ok := err.(*proto.ProtoError); ok {
		p = pe.P
	} else {
		pe = proto.NewProtoError(500, err.Error())
		p = pe.P
	}

	c.writeProto(p)
}

func (c *conn) protoError(code int, message string) error {
	return proto.NewProtoError(code, message)
}

func (c *conn) writeProto(p *proto.Proto) error {
	buf, err := proto.Marshal(p)
	if err != nil {
		return err
	}

	var n int
	c.Lock()
	n, err = c.c.Write(buf)
	c.Unlock()

	if err != nil {
		return err
	} else if n != len(buf) {
		return fmt.Errorf("write incomplete, %d less than %d", n, len(buf))
	} else {
		return nil
	}
}

func (c *conn) checkKeepAlive() {
	var f func()
	f = func() {
		if time.Now().Unix()-c.lastUpdate > int64(1.5*float32(c.app.cfg.KeepAlive)) {
			log.Info("keepalive timeout")
			c.c.Close()
			return
		} else {
			c.app.wheel.AddTask(time.Duration(c.app.cfg.KeepAlive), f)
		}
	}

	c.app.wheel.AddTask(time.Duration(c.app.cfg.KeepAlive), f)
}
