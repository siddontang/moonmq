package broker

import (
	"container/list"
	"fmt"
	"github.com/siddontang/golib/log"
	"github.com/siddontang/moonmq/proto"
	"sort"
	"sync"
	"time"
)

type routeQueue struct {
	q          *queue
	routingKey string
	msgs       []*msg

	ch chan func()

	channels *list.List

	waitingAck bool
}

func newRouteQueue(q *queue, routingKey string) *routeQueue {
	rq := new(routeQueue)

	rq.q = q
	rq.routingKey = routingKey

	rq.msgs = make([]*msg, 0, 32)

	rq.ch = make(chan func(), 32)

	rq.channels = list.New()

	rq.waitingAck = false

	return rq
}

func (rq *routeQueue) run() {
	for {
		select {
		case f := <-rq.ch:
			f()
		case <-rq.q.app.wheel.After(5 * time.Minute):
			if len(rq.msgs) == 0 && rq.channels.Len() == 0 {
				rq.q.delRouteQueue(rq.routingKey)
				return
			}

			//if 5 minutes ack not arrived, we reset and try to push agagin
			if rq.waitingAck {
				rq.waitingAck = false

				rq.push()
			}
		}
	}
}

func (rq *routeQueue) push() {
	if len(rq.msgs) == 0 {
		return
	}

	if rq.channels.Len() == 0 {
		return
	}

	if rq.waitingAck {
		//we must wait for last push ack
		return
	}

	m := rq.msgs[0]

	var err error

	switch m.pubType {
	case proto.FanoutType:
		err = rq.pushFanout(m)
	default:
		err = rq.pushDirect(m)
	}

	if err != nil {
		rq.waitingAck = true
	}
}

func (rq *routeQueue) pushDirect(m *msg) error {
	if rq.channels.Len() == 0 {
		return fmt.Errorf("no channel")
	}

	var next *list.Element
	for l := rq.channels.Front(); l != nil; l = next {
		next = l
		c := l.Value.(*channel)
		if err := c.Push(m); err == nil {
			rq.channels.Remove(l)

			rq.channels.PushBack(c)
			return nil
		} else {
			rq.channels.Remove(l)
		}
	}

	return fmt.Errorf("push direct error")
}

func (rq *routeQueue) pushFanout(m *msg) error {
	if rq.channels.Len() == 0 {
		return fmt.Errorf("no channel")
	}

	var done bool = false

	var next *list.Element
	for l := rq.channels.Front(); l != nil; l = next {
		next = l
		c := l.Value.(*channel)
		if err := c.Push(m); err == nil {
			done = true
		} else {
			rq.channels.Remove(l)
		}
	}

	if done {
		return nil
	} else {
		return fmt.Errorf("push fanout error")
	}
}

func (rq *routeQueue) Bind(ch *channel) {
	f := func() {
		for l := rq.channels.Front(); l != nil; l = l.Next() {
			if c := l.Value.(*channel); c == ch {
				return
			}
		}

		rq.push()
	}

	rq.ch <- f
}

func (rq *routeQueue) Unbind(ch *channel) {
	f := func() {
		for l := rq.channels.Front(); l != nil; l = l.Next() {
			if l.Value.(*channel) == ch {
				rq.channels.Remove(l)
				return
			}
		}
	}

	rq.ch <- f
}

func (rq *routeQueue) Push(m *msg) {
	f := func() {
		rq.msgs = append(rq.msgs, m)
		rq.push()
	}

	rq.ch <- f
}

func (rq *routeQueue) Ack(msgId int64) {
	f := func() {
		if len(rq.msgs) == 0 {
			//can not enter here
			return
		}

		if rq.msgs[0].id == msgId {
			copy(rq.msgs[0:], rq.msgs[1:])
			rq.msgs[len(rq.msgs)-1] = nil
			rq.msgs = rq.msgs[:len(rq.msgs)-1]
		}

		rq.waitingAck = false

		rq.push()
	}

	rq.ch <- f
}

type queue struct {
	sync.Mutex

	app  *App
	ms   *msgStore
	qs   *queues
	name string

	routes map[string]*routeQueue

	msgs []*msg

	ch chan func()
}

func newQueue(qs *queues, name string) *queue {
	q := new(queue)

	q.app = qs.app
	q.ms = q.app.ms
	q.qs = qs
	q.name = name

	q.routes = make(map[string]*routeQueue)

	q.ch = make(chan func(), 256)

	return q
}

func (q *queue) getRouteQueue(routingKey string) *routeQueue {
	q.Lock()
	rq, ok := q.routes[routingKey]
	if ok {
		q.Unlock()
		return rq
	} else {
		rq = newRouteQueue(q, routingKey)
		q.Unlock()
		return rq
	}
}

func (q *queue) delRouteQueue(routingKey string) {
	q.Lock()
	delete(q.routes, routingKey)
	q.Unlock()
}

func (q *queue) loadMsgs() error {
	//first get last unpushed msg
	metas, err := q.ms.RangeMeta(q.name, -(q.app.cfg.MaxQueueSize + 1), -1)
	if err != nil {
		log.Error("rang meta error %v", err)
		return err
	}

	var timeoutId int64 = 0
	now := time.Now().Unix()
	//remove timeout msg
	for i := len(metas); i > 0; i-- {
		m := metas[i-1]
		if m.ctime+int64(q.app.cfg.MessageTimeout) < now {
			timeoutId = m.id
			metas = metas[i+1:]
			break
		}
	}

	if timeoutId > 0 {
		if err := q.ms.RemRangeByID(q.name, 0, timeoutId); err != nil {
			return err
		}
	}

	//get msg data
	var datas []*msgData
	datas, err = q.ms.RangeDataByID(q.name, metas[0].id, metas[len(metas)-1].id)
	if err != nil {
		log.Error("range data error %v", err)
		return err
	}

	q.msgs = make([]*msg, 0, len(metas))
	//we may encounter this condition, body saved but meta not saved
	for i, j := 0, 0; i < len(metas) && j < len(datas); {
		m := metas[i]
		d := datas[j]
		if m.id == d.id {
			msg := newMsg(m.id, m.pubType, m.routingKey, d.body)
			q.msgs = append(q.msgs, msg)
			i++
			j++
		} else if m.id < d.id {
			i++
		} else {
			j++
		}
	}

	//dispatch msgs
	for _, m := range q.msgs {
		rq := q.getRouteQueue(m.routingKey)
		rq.Push(m)
	}

	return nil
}

func (q *queue) run() {
	defer func() {
		if e := recover(); e != nil {
			log.Fatal("queue run fatal %v", e)
		}

		q.qs.Delete(q.name)
	}()

	if err := q.loadMsgs(); err != nil {
		return
	}

	for {
		select {
		case f := <-q.ch:
			f()
		case <-q.app.wheel.After(5 * time.Minute):
			if len(q.routes) == 0 {
				return
			}
		}
	}
}

func (q *queue) Bind(ch *channel) {
	for _, k := range ch.routingKeys {
		rq := q.getRouteQueue(k)
		rq.Bind(ch)
	}
}

func (q *queue) Unbind(ch *channel) {
	for _, k := range ch.routingKeys {
		rq := q.getRouteQueue(k)
		rq.Unbind(ch)
	}
}

func (q *queue) getMsg(msgId int64) (*msg, int) {
	i := sort.Search(len(q.msgs), func(i int) bool { return q.msgs[i].id >= msgId })
	if i < len(q.msgs) && q.msgs[i].id == msgId {
		return q.msgs[i], i
	} else {
		return nil, 0
	}
}

func (q *queue) remMsg(index int) error {
	if index < 0 || index >= len(q.msgs) {
		return fmt.Errorf("invalid index")
	}

	m := q.msgs[index]

	if err := q.ms.RemRangeByID(q.name, m.id, m.id); err != nil {
		return err
	}

	copy(q.msgs[index:], q.msgs[index+1:])
	q.msgs[len(q.msgs)-1] = nil
	q.msgs = q.msgs[:len(q.msgs)-1]

	return nil
}

func (q *queue) Ack(msgId int64) {
	f := func() {
		m, i := q.getMsg(msgId)
		if m == nil {
			return
		}

		if err := q.remMsg(i); err != nil {
			log.Error("remove msg error %v", err)
			return
		}

		routingKey := m.routingKey
		rq := q.getRouteQueue(routingKey)

		rq.Ack(msgId)
	}

	q.ch <- f
}

func (q *queue) Push(m *msg) {
	f := func() {
		if err := q.ms.Save(q.name, m); err != nil {
			log.Error("save msg error %v", err)
			return
		}

		q.msgs = append(q.msgs, m)

		if len(q.msgs) > q.app.cfg.MaxQueueSize {
			dm := q.msgs[0]

			if err := q.remMsg(0); err != nil {
				log.Error("remove msg error %v", err)
				return
			}

			rk := dm.routingKey
			rq := q.getRouteQueue(rk)

			//we treat as ack
			rq.Ack(dm.id)
		}

		routingKey := m.routingKey
		rq := q.getRouteQueue(routingKey)

		rq.Push(m)
	}

	q.ch <- f
}

type queues struct {
	sync.Mutex
	app *App

	qs map[string]*queue
}

func newQueues(app *App) *queues {
	qs := new(queues)

	qs.app = app
	qs.qs = make(map[string]*queue)

	return qs
}

func (qs *queues) Get(name string) *queue {
	qs.Lock()
	if q, ok := qs.qs[name]; ok {
		qs.Unlock()
		return q
	} else {
		q = newQueue(qs, name)

		qs.qs[name] = q
		qs.Unlock()

		go q.run()

		return q
	}
}

func (qs *queues) Delete(name string) {
	qs.Lock()
	delete(qs.qs, name)
	qs.Unlock()
}
