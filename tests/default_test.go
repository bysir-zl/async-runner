package tests

import (
	"github.com/bysir-zl/async-runner/client"
	"github.com/bysir-zl/async-runner/server"
	"github.com/bysir-zl/bygo/log"
	"testing"
)


func TestHttpServer(t *testing.T) {
	s := server.NewServer()
	err := s.Start()
	t.Error(err)
}

func TestWorker(t *testing.T) {
	c := client.NewHttpReceiver(":9999")
	c.AddListener("test", func(data []byte) (err error) {
		log.Info("test", "runned1", data)
		return
	})
	c.AddListener("test1", func(data []byte)(err error) {
		log.Info("test1", "runned2", string(data))
		return
	})

	c.StartServer()

}

func TestHttpClintPush(t *testing.T) {
	c := client.NewHttpPusher("http://127.0.0.1:9989", "http://127.0.0.1:9999")

	c.Push("test", 1, []byte{1,10})
	//c.Push("test1", 1, []byte(`{"b":1}`))
}

func BenchmarkPush(b *testing.B) {
	c := client.NewHttpPusher("http://127.0.0.1:9989", "http://127.0.0.1:9999")


	for i := 0; i < b.N; i++ {
		c.Push("test", 1, []byte{1,10})
	}
}