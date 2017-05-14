package main

import (
	"github.com/bysir-zl/async-runner/server"
	"io/ioutil"
	"github.com/bysir-zl/orm"
	"github.com/bysir-zl/async-runner/core"
	"encoding/json"
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
	err = json.Unmarshal(bs,&c)
	if err != nil {
		panic(err)
	}

	if  c.Persistence{
		orm.RegisterDb("default", "mysql", c.MysqlLink)
		orm.RegisterModel((*core.JobModel)(nil))
	}
}
