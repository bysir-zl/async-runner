package tests

import (
	"github.com/bysir-zl/async-runner/client"
	"github.com/bysir-zl/async-runner/server"
	"github.com/bysir-zl/bygo/log"
	"testing"
)


func TestHttpServer(t *testing.T) {
	s := server.NewServer()
	s.Start()
}

func TestWorker(t *testing.T) {
	c := client.NewClientHttp()
	c.AddListener("test", func(data []byte) (err error) {
		log.Info("test", "runned1", data)
		return
	})
	c.AddListener("test1", func(data []byte)(err error) {
		log.Info("test1", "runned2", string(data))
		return
	})

	c.StartClientHttp()

	var i chan int
	<-i
}

func TestHttpClintPush(t *testing.T) {
	c := client.NewClientHttp()

	c.Push("test", 171, []byte{1,10})
	//c.Push("test1", 1, []byte(`{"b":1}`))
}
