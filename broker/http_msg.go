package broker

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

type MsgHandler struct {
	app *App
}

func newMsgHandler(app *App) *MsgHandler {
	h := new(MsgHandler)

	h.app = app

	return h
}

func (h *MsgHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		h.publishMsg(w, r)
	case "PUT":
		h.publishMsg(w, r)
	case "GET":
		h.getMsg(w, r)
	default:
		http.Error(w, "invalid http method", http.StatusMethodNotAllowed)
	}
}

func (h *MsgHandler) publishMsg(w http.ResponseWriter, r *http.Request) {
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	queue := r.FormValue("queue")
	routingKey := r.FormValue("routing_key")
	tp := r.FormValue("pub_type")

	if err := checkPublish(queue, routingKey, tp, message); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var m *msg
	m, err = h.app.saveMsg(queue, routingKey, tp, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	q := h.app.qs.Get(queue)
	q.Push(m)

	w.Write([]byte(strconv.FormatInt(m.id, 10)))
}

type httpMsgPusher struct {
	m chan *msg
	e chan error
}

func (p *httpMsgPusher) Push(ch *channel, m *msg) error {
	p.m <- m

	e, ok := <-p.e
	if e != nil {
		return e
	} else if !ok {
		return fmt.Errorf("push invalid channel")
	} else {
		return nil
	}
}

func (h *MsgHandler) getMsg(w http.ResponseWriter, r *http.Request) {
	queue := r.FormValue("queue")
	routingKey := r.FormValue("routing_key")

	if err := checkBind(queue, routingKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mc := make(chan *msg, 1)
	ec := make(chan error, 1)
	q := h.app.qs.Get(queue)
	ch := newChannel(&httpMsgPusher{mc, ec}, q, routingKey, true)
	defer ch.Close()

	select {
	case m := <-mc:
		_, err := w.Write(m.body)
		if err == nil && ch.noAck {
			ch.Ack(m.id)
		}

		ec <- err
		close(ec)
	case <-time.After(60 * time.Second):
		w.WriteHeader(http.StatusNoContent)
	}
}
