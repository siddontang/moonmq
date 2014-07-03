package broker

import (
	"fmt"
	"github.com/siddontang/go-log/log"
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

	channels map[string]*channel
}

func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.decoder = proto.NewDecoder(co)

	c.checkKeepAlive()

	c.channels = make(map[string]*channel)

	return c
}

func (c *conn) run() {
	c.onRead()

	c.unBindAll()

	c.c.Close()
}

func (c *conn) unBindAll() {
	for _, ch := range c.channels {
		ch.Close()
	}

	c.channels = map[string]*channel{}
}

func (c *conn) onRead() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			log.Fatal("crash %v:%s", err, buf)
		}
	}()

	for {
		p, err := c.decoder.Decode()
		if err != nil {
			if err != io.EOF {
				log.Info("on read error %v", err)
			}
			return
		}

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

		if err != nil {
			c.writeError(err)
		}
	}
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
			time.AfterFunc(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
		}
	}

	time.AfterFunc(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
}
