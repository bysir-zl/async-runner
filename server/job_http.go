package server

import (
	"errors"
	"github.com/valyala/fasthttp"
)

type JobHttp struct {
	callback, topic string
	data            []byte
}

func (p *JobHttp) Run() (err error) {
	args := fasthttp.Args{}
	// fasthttp有点奇葩, post只能是键值对
	args.SetBytesV("data", p.data)
	_, body, err := fasthttp.Post(nil, p.callback+"/do_job?topic="+p.topic, &args)
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

func NewJobHttpClient(callback, topic string, data []byte) Job {
	job := &JobHttp{
		callback: callback,
		topic:    topic,
		data:     data,
	}
	return job
}
