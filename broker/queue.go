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

	store Store

	queue      string
	routingKey string

	conns *list.List

	ch chan func()

	waitingAcks map[*conn]struct{}
	lastPushId  int64
}

func newRouteQueue(qs *queues, queue string, routingkey string) *routeQueue {
	rq := new(routeQueue)

	rq.qs = qs
	rq.app = qs.app

	rq.store = qs.app.ms

	rq.queue = queue
	rq.routingKey = routingkey

	rq.conns = list.New()

	rq.lastPushId = -1

	rq.waitingAcks = make(map[*conn]struct{})

	rq.ch = make(chan func(), 32)

	go rq.run()

	return rq
}

func (rq *routeQueue) run() {
	for {
		select {
		case f := <-rq.ch:
			f()
		case <-rq.app.wheel.After(5 * time.Minute):
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
		var repush bool = false
		for e := rq.conns.Front(); e != nil; e = e.Next() {
			if e.Value.(*conn) == c {
				rq.conns.Remove(e)

				if _, ok := rq.waitingAcks[c]; ok {
					//conn not ack
					delete(rq.waitingAcks, c)

					if len(rq.waitingAcks) == 0 {
						//all waiting conn not send ack
						//repush
						repush = true
					}
				}
				break
			}
		}
		if repush {
			rq.lastPushId = -1
			rq.push()
		}
	}

	rq.ch <- f
}

func (rq *routeQueue) Ack(msgId int64) {
	f := func() {
		if msgId != rq.lastPushId {
			return
		}

		rq.store.Delete(rq.queue, rq.routingKey, msgId)

		rq.waitingAcks = map[*conn]struct{}{}
		rq.lastPushId = -1

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

		if rq.app.cfg.MessageTimeout > 0 {
			now := time.Now().Unix()
			if m.ctime+int64(rq.app.cfg.MessageTimeout) < now {
				if err := rq.store.Delete(rq.queue, rq.routingKey, m.id); err != nil {
					return nil, err
				}
			} else {
				break
			}
		}
	}

	return m, nil
}

func (rq *routeQueue) push() {
	if rq.lastPushId != -1 {
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
		rq.lastPushId = m.id
	}
}

func (rq *routeQueue) pushMsg(done chan bool, m *msg, conn *conn) {
	go func() {
		if err := conn.Push(rq.queue, rq.routingKey, m); err == nil {
			//push suc
			done <- true
		} else {
			done <- false
		}
	}()
}

func (rq *routeQueue) pushDirect(m *msg) error {
	e := rq.conns.Front()

	c := e.Value.(*conn)

	rq.conns.Remove(e)
	rq.conns.PushBack(c)

	rq.waitingAcks[c] = struct{}{}

	done := make(chan bool, 1)

	rq.pushMsg(done, m, c)

	if r := <-done; r == true {
		return nil
	} else {
		return fmt.Errorf("push direct error")
	}
}

func (rq *routeQueue) pushFanout(m *msg) error {
	done := make(chan bool, rq.conns.Len())

	for e := rq.conns.Front(); e != nil; e = e.Next() {
		c := e.Value.(*conn)
		rq.waitingAcks[c] = struct{}{}

		rq.pushMsg(done, m, c)
	}

	for i := 0; i < rq.conns.Len(); i++ {
		r := <-done
		if r == true {
			return nil
		}
	}

	return fmt.Errorf("push fanout error")
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
