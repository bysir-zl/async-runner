package client

import (
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"net/http"
	"strings"
)

type Listener func(body []byte) error

type HttpReceiver struct {
	listeners  map[string]Listener
	listenAddr string
}

type HttpPusher struct {
	callbackAddr string
	serverAddr   string
}

// 在startServer后会监听listenAddr地址
func NewHttpReceiver(listenAddr string) *HttpReceiver {
	return &HttpReceiver{
		listeners:  map[string]Listener{},
		listenAddr: listenAddr,
	}
}

// callbackAddr 服务器地址，callbackAddr 回调地址 同时会监听回调地址以实现Listener功能
func NewHttpPusher(serverAddr, callbackAddr string) *HttpPusher {
	if serverAddr == "" || callbackAddr == "" {
		panic(errors.New("serverAddr or callbackAddr can't be empty"))
	}
	return &HttpPusher{
		callbackAddr: callbackAddr,
		serverAddr:   serverAddr,
	}
}

// 开启阻塞服务
func (p *HttpReceiver) StartServer() (err error) {
	// host/do_job?topic=1234 -d="[]byte"
	if p.listenAddr == "" {
		err = errors.New("listenAddr is empty")
		return
	}
	err = fasthttp.ListenAndServe(p.listenAddr, func(ctx *fasthttp.RequestCtx) {
		uri := ctx.Request.URI()
		path := string(uri.Path())
		pathS := strings.Split(path, "/")
		args := uri.QueryArgs()
		topic := string(args.Peek("topic"))

		data := ctx.Request.PostArgs().Peek("data")

		if pathS[1] == "do_job" {
			err := p.Commit(topic, data)
			if err != nil {
				ctx.WriteString(err.Error())
				return
			}
		}

		ctx.WriteString("success")
		return
	})

	return
}

func (p *HttpReceiver) AddListener(topic string, listener Listener) {
	p.listeners[topic] = listener
}

func (p *HttpReceiver) Commit(topic string, data []byte) (err error) {
	fun, ok := p.listeners[topic]
	if !ok {
		err = errors.New("no topic named :" + topic)
		return
	}
	err = fun(data)
	if err != nil {
		return
	}

	return
}

// 处理来自的async-runner的http回调请求
// 最后直接响应response就行了
func (p *HttpReceiver) HandleRequest(req *http.Request) (response string) {
	req.ParseForm()

	path := req.URL.Path
	pathS := strings.Split(path, "/")
	if len(pathS) <= 1 {
		response = "bad path:" + path
		return
	}
	topic := req.FormValue("topic")

	data := []byte(req.FormValue("data"))

	action := pathS[len(pathS)-1]
	if action == "do_job" {
		err := p.Commit(topic, data)
		if err != nil {
			response = err.Error()
			return
		}
	} else {
		response = "bad action:" + action
		return
	}

	response = "success"
	return
}

// HttpPusher

func (p *HttpPusher) Add(topic string, timeout int64, data []byte) (err error) {
	arg := fasthttp.Args{}
	arg.SetBytesV("data", data)
	_, _, err = fasthttp.Post(nil, fmt.Sprintf("%s/add?topic=%s&timeout=%d&callback=%s", p.serverAddr, topic, timeout, p.callbackAddr), &arg)
	if err != nil {
		return
	}
	return
}

func (p *HttpPusher) DeleteThenAdd(topic string, timeout int64, data []byte) (err error) {
	arg := fasthttp.Args{}
	arg.SetBytesV("data", data)
	_, _, err = fasthttp.Post(nil, fmt.Sprintf("%s/delete_then_add?topic=%s&timeout=%d&callback=%s", p.serverAddr, topic, timeout, p.callbackAddr), &arg)
	if err != nil {
		return
	}
	return
}

func (p *HttpPusher) Delete(topic string, data []byte) (err error) {
	arg := fasthttp.Args{}
	arg.SetBytesV("data", data)
	_, _, err = fasthttp.Post(nil, fmt.Sprintf("%s/delete?topic=%s&callback=%s", p.serverAddr, topic, p.callbackAddr), &arg)
	if err != nil {
		return
	}
	return
}

func init() {

}
