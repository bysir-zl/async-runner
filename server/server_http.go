package server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/bysir-zl/async-runner/core"
	"github.com/bysir-zl/bygo/log"
	"github.com/valyala/fasthttp"
	"strconv"
	"strings"
	"sync"
)

type HttpServer struct {
	s *core.Scheduler
	c *core.SchedulerConfig
}

func newJobFunc() core.Job {
	return &JobHttp{}
}

func NewHttpServer(c *core.SchedulerConfig) *HttpServer {
	return &HttpServer{
		s: core.NewScheduler(c, newJobFunc),
		c: c,
	}
}

func (p *HttpServer) Start() (err error) {
	go p.s.Work()
	log.Info("server_http", "start server success")
	err = fasthttp.ListenAndServe(p.c.ServerHttp, func(ctx *fasthttp.RequestCtx) {
		p.handlerQuery(ctx)
		return
	})
	return
}

var jobPull = sync.Pool{
	New: func() interface{} {
		return new(JobHttp)
	},
}

func (p *HttpServer) handlerQuery(ctx *fasthttp.RequestCtx) {
	req := ctx.Request
	uri := req.URI()
	path := string(uri.Path())
	pathS := strings.Split(path, "/")
	action := pathS[1]

	args := uri.QueryArgs()
	topic := string(args.Peek("topic"))
	callback := string(args.Peek("callback"))
	timeout := string(args.Peek("timeout"))

	form := req.PostArgs()
	data := form.Peek("data")

	timeoutInt, _ := strconv.ParseInt(timeout, 10, 64)

	switch action {
	case "add":
		// 添加一个工作
		p.s.AddJob(timeoutInt, NewJobHttpClient(callback, topic, data))
	case "delete_then_add":
		// 删除一个并添加
		p.s.DeleteThenAddJob(timeoutInt, NewJobHttpClient(callback, topic, data))
	case "delete":
		p.s.DeleteJob(NewJobHttpClient(callback, topic, data))
	case "info":
		info := p.s.Info()
		ctx.Response.Header.Add("content-type", "json")
		ctx.WriteString(info)
	}
}

// job

type JobHttp struct {
	callback, topic string
	data            []byte
}

var sp = "@.@"

var defaultClient fasthttp.Client

func (p *JobHttp) String() string {
	return fmt.Sprintf("topic:%s", p.topic)
}

func (p *JobHttp) Unmarshal(bs []byte) error {
	ds := bytes.Split(bs, []byte(sp))
	if len(ds) != 3 {
		return errors.New("data error")
	}
	p.topic = string(ds[0])
	p.callback = string(ds[1])
	p.data = ds[2]

	return nil
}

func (p *JobHttp) Marshal() ([]byte, error) {
	bf := bytes.Buffer{}
	bf.WriteString(p.topic + sp + p.callback + sp)
	bf.Write(p.data)
	return bf.Bytes(), nil
}

func (p *JobHttp) Unique() []byte {
	d, _ := p.Marshal()
	return d
}

func (p *JobHttp) Run() (err error) {
	args := fasthttp.Args{}
	// fasthttp有点奇葩, post只能是键值对
	args.SetBytesV("data", p.data)

	_, body, err := defaultClient.Post(nil, p.callback+"/do_job?topic="+p.topic, &args)
	if err != nil {
		return
	}

	bodyString := string(body)
	if bodyString != "success" {
		err = errors.New("client response is not 'success', is " + bodyString)
		return
	}
	return nil
}

func NewJobHttpClient(callback, topic string, data []byte) core.Job {
	job := &JobHttp{
		callback: callback,
		topic:    topic,
		data:     data,
	}
	return job
}

func init() {
	defaultClient.MaxConnsPerHost = 60000
}
