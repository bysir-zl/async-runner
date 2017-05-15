package main

import (
	"encoding/json"
	"github.com/bysir-zl/async-runner/core"
	"github.com/bysir-zl/async-runner/server"
	"github.com/bysir-zl/orm"
	"io/ioutil"
)

var c core.SchedulerConfig

func main() {
	s := server.NewHttpServer(&c)
	s.Start()
}

func init() {
	// 加载配置
	bs, err := ioutil.ReadFile("./config.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bs, &c)
	if err != nil {
		panic(err)
	}

	if c.Log {
		orm.RegisterDb("default", "mysql", c.MysqlLink)
		orm.RegisterModel(new(core.JobModel))
	}
}
