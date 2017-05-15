package core

import (
	"bytes"
	"errors"
	"strconv"
)

type Job interface {
	Run() error
	String() string
	Unmarshal([]byte) error   // 反序列化
	Marshal() ([]byte, error) // 序列化
	Unique() []byte           // 必须返回一个[]byte,将做为唯一标示,相同唯一标示的job会替换已存在的job
}

// 包裹JOB
type JobWrap struct {
	job Job

	IdDb  string // 入库的ID，唯一标识
	Deep  int64  // 圈数，一圈1小时
	Count int  // 第几次运行

	RunTime int64 // 运行时间戳,只在持久化时入库出库使用
}

var ps = []byte("JW@.@JW")
var tableNameUndoJob = "undo"

func (p *JobWrap) Unmarshal(data []byte) (err error) {
	bs := bytes.Split(data, ps)
	if len(bs) != 2 {
		err = errors.New("format error")
		return
	}

	runTime, _ := strconv.ParseInt(string(bs[0]), 10, 64)
	p.RunTime = runTime
	err = p.job.Unmarshal(bs[1])
	if err != nil {
		return
	}
	return
}

func (p *JobWrap) Marshal() (data []byte, err error) {
	var bf bytes.Buffer
	job, err := p.job.Marshal()
	if err != nil {
		return
	}
	bf.WriteString(strconv.FormatInt(p.RunTime, 10))
	bf.Write(ps)
	bf.Write(job)
	data = bf.Bytes()
	return
}
