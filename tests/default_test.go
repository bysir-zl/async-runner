package tests

import (
	"encoding/json"
	"github.com/bysir-zl/async-runner/client"
	"github.com/bysir-zl/async-runner/core"
	"github.com/bysir-zl/async-runner/server"
	"github.com/bysir-zl/bygo/log"
	"github.com/bysir-zl/orm"
	"io/ioutil"
	"runtime"
	"strings"
	"testing"
	"time"
)

var c core.SchedulerConfig

// 1. 先运行服务
func TestHttpServer(t *testing.T) {
	s := server.NewHttpServer(&c)
	err := s.Start()
	t.Error(err)
}

// 2. 运行worker以接收回调
func TestWorker(t *testing.T) {
	c := client.NewHttpReceiver(":9999")
	c.AddListener("test", func(data []byte) (err error) {
		log.Info("test", "runned1", data)
		return
	})
	c.AddListener("test1", func(data []byte) (err error) {
		log.Info("test1", "runned2", string(data))
		return
	})

	c.StartServer()
}

// 3. 模拟发送一个job
func TestHttpClintPush(t *testing.T) {
	c := client.NewHttpPusher("http://127.0.0.1:9989", "http://127.0.0.1:9999")

	n := time.Now()
	for i := 0; i < 10000; i++ {
		c.Add("test", 1, []byte{1, 10})
	}
	log.Info("time", time.Now().Sub(n))
	//c.Delete("test", []byte{1, 10})
	//c.DeleteThenAdd("test", 10, []byte{1, 10})
	//c.Delete("test", []byte{1, 10})
	//c.Add("test1", 1, []byte(`{"b":1}`))
}

// 26284002 ns/op  同步调用数据库
// 1271805 ns/op  改为异步后
// 361076 ns/op 改为redis
func BenchmarkPush(b *testing.B) {
	c := client.NewHttpPusher("http://127.0.0.1:9989", "http://127.0.0.1:9999")

	for i := 0; i < b.N; i++ {
		c.Add("test", 1, []byte{1, 10})
	}
}

func init() {
	// 加载配置
	_, f, _, _ := runtime.Caller(1)
	abs := strings.Split(f, "/tests/")[0]
	if abs == "" {
		abs = "."
	}

	configPath := abs + "/config.json"
	bs, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bs, &c)
	if err != nil {
		panic(err)
	}

	orm.Debug = true
}
