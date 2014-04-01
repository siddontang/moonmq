package broker

import (
	"container/list"
	"fmt"
	"github.com/siddontang/moonmq/proto"
	"sync"
	"time"
)

type routeQueue struct {
	qs *queues

	app *App

	store *msgStore

	queue      string
	routingKey string

	conns *list.List

	ch chan func()

	waitingAck bool

	lastWaitingAck int64
}

func newRouteQueue(qs *queues, queue string, routingkey string) *routeQueue {
	rq := new(routeQueue)

	rq.qs = qs
	rq.app = qs.app

	rq.store = qs.app.ms

	rq.queue = queue
	rq.routingKey = routingkey

	rq.conns = list.New()

	rq.waitingAck = false

	rq.ch = make(chan func(), 32)

	go rq.run()

	return rq
}

func (rq *routeQueue) run() {
	for {
		select {
		case f := <-rq.ch:
			f()
		case <-rq.app.wheel.After(1 * time.Minute):
			if rq.waitingAck {
				if time.Now().Unix()-rq.lastWaitingAck > int64(1*time.Minute) {
					rq.waitingAck = false
				}
			}
			if rq.conns.Len() == 0 {
				m, _ := rq.getMsg()
				if m == nil {
					//no conn, and no msg
					rq.qs.Delete(rq.queue, rq.routingKey)
					return
				}
			}
		}
	}
}

func (rq *routeQueue) Bind(c *conn) {
	f := func() {
		for e := rq.conns.Front(); e != nil; e = e.Next() {
			if e.Value.(*conn) == c {
				return
			}
		}

		rq.conns.PushBack(c)

		rq.push()
	}

	rq.ch <- f
}

func (rq *routeQueue) Unbind(c *conn) {
	f := func() {
		for e := rq.conns.Front(); e != nil; e = e.Next() {
			if e.Value.(*conn) == c {
				rq.conns.Remove(e)
				return
			}
		}
	}

	rq.ch <- f
}

func (rq *routeQueue) Ack(msgId int64) {
	f := func() {
		rq.store.Delete(rq.queue, rq.routingKey, msgId)

		rq.waitingAck = false

		rq.push()
	}

	rq.ch <- f
}

func (rq *routeQueue) Push(m *msg) {
	f := func() {
		rq.push()
	}

	rq.ch <- f
}

func (rq *routeQueue) getMsg() (*msg, error) {
	var m *msg
	var err error
	for {
		m, err = rq.store.Front(rq.queue, rq.routingKey)
		if err != nil {
			return nil, err
		} else if m == nil {
			return nil, nil
		}

		now := time.Now().Unix()
		if m.ctime+int64(rq.app.cfg.MessageTimeout) < now {
			if err := rq.store.Delete(rq.queue, rq.routingKey, m.id); err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	return m, nil
}

func (rq *routeQueue) push() {
	if rq.waitingAck {
		return
	}

	if rq.conns.Len() == 0 {
		return
	}

	m, err := rq.getMsg()
	if err != nil {
		return
	} else if m == nil {
		return
	}

	switch m.pubType {
	case proto.FanoutType:
		err = rq.pushFanout(m)
	default:
		err = rq.pushDirect(m)
	}

	if err == nil {
		rq.waitingAck = true

		rq.lastWaitingAck = time.Now().Unix()
	}
}

func (rq *routeQueue) pushDirect(m *msg) error {
	var next *list.Element
	for e := rq.conns.Front(); e != nil; e = next {
		next = e.Next()

		c := e.Value.(*conn)
		if err := c.Push(rq.queue, rq.routingKey, m); err != nil {
			rq.conns.Remove(e)
		} else {
			rq.conns.Remove(e)
			rq.conns.PushBack(c)
			return nil
		}
	}

	return fmt.Errorf("push direct error")
}

func (rq *routeQueue) pushFanout(m *msg) error {
	var done bool = false
	var next *list.Element
	for e := rq.conns.Front(); e != nil; e = next {
		next = e.Next()

		c := e.Value.(*conn)

		if err := c.Push(rq.queue, rq.routingKey, m); err != nil {
			rq.conns.Remove(e)
		} else {
			done = true
		}
	}

	if done {
		return nil
	} else {
		return fmt.Errorf("push fanout error")
	}
}

type queues struct {
	sync.Mutex
	app *App

	routes map[string]*routeQueue
}

func newQueues(app *App) *queues {
	qs := new(queues)

	qs.app = app
	qs.routes = make(map[string]*routeQueue)

	return qs
}

func (qs *queues) Get(queue string, routingKey string) *routeQueue {
	key := fmt.Sprintf("%s-%s", queue, routingKey)

	qs.Lock()
	if r, ok := qs.routes[key]; ok {
		qs.Unlock()
		return r
	} else {
		r := newRouteQueue(qs, queue, routingKey)
		qs.routes[key] = r
		qs.Unlock()
		return r
	}
}

func (qs *queues) Getx(queue string, routingKey string) *routeQueue {
	key := fmt.Sprintf("%s-%s", queue, routingKey)

	qs.Lock()
	r, ok := qs.routes[key]
	qs.Unlock()

	if ok {
		return r
	} else {
		return nil
	}

}

func (qs *queues) Delete(queue string, routingKey string) {
	key := fmt.Sprintf("%s-%s", queue, routingKey)

	qs.Lock()
	delete(qs.routes, key)
	qs.Unlock()
}
