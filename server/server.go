package server

import (
	"github.com/bysir-zl/bygo/log"
	"github.com/valyala/fasthttp"
	"strconv"
	"strings"
)

type Server struct {
	s *Scheduler
}

func NewServer() *Server {
	return &Server{
		s: NewScheduler(),
	}
}

func (p *Server) Start() {
	go p.s.Work()
	fasthttp.ListenAndServe(":9989", func(ctx *fasthttp.RequestCtx) {
		p.handlerQuery(ctx)
		return
	})
}

func (p *Server) handlerQuery(ctx *fasthttp.RequestCtx) {
	req := ctx.Request
	uri := req.URI()
	path := string(uri.Path())
	pathS := strings.Split(path, "/")
	action := pathS[1]

	args := uri.QueryArgs()
	topic := string(args.Peek("topic"))
	callback := string(args.Peek("callback"))
	timeout := string(args.Peek("timeout"))

	//var data []byte
	form := req.PostArgs()
	data := form.Peek("data")

	timeoutInt, _ := strconv.ParseInt(timeout, 10, 64)

	if action == "push" {
		// 添加一个工作
		log.Info("runner-server", "add job, topic:", topic)
		p.s.addJob(timeoutInt, NewJobHttpClient(callback, topic, data),0)
	}
}
