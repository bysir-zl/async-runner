package client

import (
	"fmt"
	"github.com/bysir-zl/bygo/log"
	"github.com/valyala/fasthttp"
	"strings"
)

type ClientHttp struct {
	Listeners map[string]Listener
}

type Listener func(body []byte) error

func NewClientHttp() *ClientHttp {
	return &ClientHttp{
		Listeners: map[string]Listener{},
	}
}

func (p *ClientHttp) StartClientHttp() {
	go fasthttp.ListenAndServe(":9999", func(ctx *fasthttp.RequestCtx) {
		uri := ctx.Request.URI()
		path := string(uri.Path())
		pathS := strings.Split(path, "/")
		args := uri.QueryArgs()
		topic := string(args.Peek("topic"))

		data := ctx.Request.PostArgs().Peek("data")

		if pathS[1] == "do_job" {
			err := p.Listeners[topic](data)
			if err != nil {
				ctx.WriteString(err.Error())
				return
			}
		}

		ctx.WriteString("success")
		return
	})

}

func (p *ClientHttp) AddListener(topic string, listener Listener) {
	p.Listeners[topic] = listener
}

func (p *ClientHttp) Push(topic string, timeout int64, data []byte) {
	arg := fasthttp.Args{}
	arg.SetBytesV("data", data)
	_, _, err := fasthttp.Post(nil, fmt.Sprintf("http://127.0.0.1:9989/push?topic=%s&timeout=%d&callback=http://127.0.0.1:9999/", topic, timeout), &arg)
	if err != nil {
		log.Error("runner-client", "push job error:", err)
	}
}

func init() {

}
