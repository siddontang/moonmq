package broker

import (
	"bytes"
	"fmt"
	"github.com/siddontang/golib/log"
	"github.com/siddontang/moonmq/proto"
	"io"
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

	routes map[string][]string

	noAcks map[string]struct{}

	authed bool
}

func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.authed = false

	c.decoder = proto.NewDecoder(co)

	c.checkKeepAlive()

	c.routes = make(map[string][]string)
	c.noAcks = make(map[string]struct{})

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

		c.unbindAll()

		c.c.Close()

	}()

	for {
		p, err := c.decoder.Decode()
		if err != nil {
			if err != io.EOF {
				log.Info("on read error %v", err)
			}
			return
		}

		if p.Method == proto.Auth {
			err = c.handleAuth(p)
		} else {
			if len(c.app.passMD5) > 0 && !c.authed {
				err = fmt.Errorf("must auth first")
			} else {
				switch p.Method {
				case proto.Publish:
					err = c.handlePublish(p)
				case proto.Bind:
					err = c.handleBind(p)
				case proto.Unbind:
					err = c.handleUnbind(p)
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

func (c *conn) handleAuth(p *proto.Proto) error {
	if len(c.app.passMD5) > 0 {
		if !bytes.Equal(p.Body, c.app.passMD5) {
			return fmt.Errorf("invalid password")
		}
	}

	c.authed = true

	rp := proto.NewAuthOKProto()

	c.writeProto(rp.P)

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
		c.c.Close()
		return err
	} else if n != len(buf) {
		c.c.Close()
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
			c.app.wheel.AddTask(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
		}
	}

	c.app.wheel.AddTask(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
}
