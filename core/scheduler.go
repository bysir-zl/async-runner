package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/bysir-zl/bygo/log"
	"github.com/bysir-zl/orm"
	"sync"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	Tasks      [3600]*Task `json:"tasks"`  // 3600个task 每一秒需要执行一个
	CurrIndex  int32 `json:"curr_index"`   // 当前正在执行哪个task
	config     *SchedulerConfig `json:"-"` // 是否使用持久化
	newJobFunc NewJobFunc `json:"-"`
	sync.Mutex // 锁全部Task
}

type Task struct {
	JobWraps []*JobWrap // 当前task里的任务
	sync.Mutex          // 锁一个Task
}

type SchedulerConfig struct {
	ServerHttp  string `json:"server_http"`
	Persistence bool  `json:"persistence"` // 是否使用redis持久化
	Redis       string `json:"redis"`      // redis
	Log         bool `json:"log"`          // 是否记录日志
	MysqlLink   string `json:"mysql"`      // mysql
	Retry       []int `json:"retry"`       // 重试延时
}

type NewJobFunc func() Job

func NewScheduler(c *SchedulerConfig, newJobFunc NewJobFunc) *Scheduler {
	s := &Scheduler{
		config:     c,
		newJobFunc: newJobFunc,
	}
	for i := 0; i < 3600; i++ {
		s.Tasks[i] = &Task{
			JobWraps: []*JobWrap{},
		}
	}

	if c.Persistence {
		orm.RegisterDb("default", "mysql", c.MysqlLink)
		orm.RegisterModel(new(JobModel))

		InitPersistence(c.Redis)

		err := s.LoadFormRedis()
		if err != nil {
			log.Warn("run", err)
		}
	}

	return s
}

// 从db装入job
func (p *Scheduler) LoadFormRedis() (err error) {
	jobWraps, err := readAllUndoJobs(p.newJobFunc)
	if err != nil {
		return
	}

	if jobWraps == nil || len(*jobWraps) == 0 {
		return
	}

	now := time.Now()
	for _, jobWrap := range *jobWraps {
		d := int64(time.Unix(jobWrap.RunTime, 0).Sub(now).Seconds())
		p.rollbackJobWrap(d, jobWrap)
	}

	return
}

// 开启工作循环
func (p *Scheduler) Work() {
	d := time.Second
	t := time.NewTicker(d).C
	for range t {
		go p.doCurrJobsAndNext()
	}
}

func (p *Scheduler) GetCurrJobWraps() *[]*JobWrap {
	c := atomic.LoadInt32(&p.CurrIndex)
	jobs := &p.Tasks[c].JobWraps
	return jobs
}

// 删除所有相同job
func (p *Scheduler) DeleteJob(job Job) (ok bool) {
	p.Lock()
	deletedIds := []string{}
	for i := 0; i < 3600; i++ {
		jobWraps := &p.Tasks[i].JobWraps
		deletedCount := 0
		for j := range *jobWraps {
			wrap := (*jobWraps)[j-deletedCount]
			if bytes.Equal(wrap.job.Unique(), job.Unique()) {
				deletedIds = append(deletedIds, wrap.IdDb)
				*jobWraps = append((*jobWraps)[:j-deletedCount], (*jobWraps)[j+1-deletedCount:]...)
				deletedCount++
			}
		}
	}
	p.Unlock()

	l := len(deletedIds)
	log.Info("runner", "job deleted: (%v), count: %d", job, l)

	ok = l == 0

	if p.config.Persistence {
		_, err := deleteJobs(deletedIds)
		if err != nil {
			log.Error("runner-pers", err)
		}
	}
	return
}

// 删除所有相同的job后添加一个新job
func (p *Scheduler) DeleteThenAddJob(duration int64, job Job) (deleted bool) {
	deleted = p.DeleteJob(job)
	p.AddJob(duration, job)

	return
}

// 秒为单位
func (p *Scheduler) AddJob(duration int64, job Job) {
	if duration < 0 {
		duration = 0
	}

	deep := duration / 3600
	index := int32(duration%3600) + atomic.LoadInt32(&p.CurrIndex)
	if index >= 3600 {
		index = index % 3600
		deep ++
	}

	jobWrap := &JobWrap{Deep: deep, job: job}

	log.Info("runner", "job added: (%v), duration: %ds", job, duration)

	if duration == 0 {
		p.doJobs(&[]*JobWrap{jobWrap})
		return
	}

	p.Tasks[index].Lock()
	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, jobWrap)
	p.Tasks[index].Unlock()

	// 持久化
	if p.config.Persistence {
		jobWrap.RunTime = time.Now().Add(time.Duration(duration) * time.Second).Unix()
		err := addJob(jobWrap)
		if err != nil {
			log.Error("runner-pers", err)
		}
	}
}

// 回滚一个job, 秒为单位
func (p *Scheduler) rollbackJobWrap(duration int64, jobWrap *JobWrap) {
	if duration < 0 {
		duration = 0
	}
	deep := duration / 3600
	index := int32(duration%3600) + atomic.LoadInt32(&p.CurrIndex)
	if index >= 3600 {
		index = index % 3600
		deep ++
	}

	jobWrap.Deep = deep
	if duration == 0 {
		p.doJobs(&[]*JobWrap{jobWrap})
		return
	}

	p.Tasks[index].Lock()
	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, jobWrap)
	p.Tasks[index].Unlock()
}

func (p *Scheduler) doJob(jobWrap *JobWrap) (err error) {
	workC := make(chan error)
	timeout := time.Second * 8

	go func() {
		workC <- jobWrap.job.Run()
	}()

	select {
	case err = <-workC:
		if err != nil {
			return
		}
	case <-time.After(timeout):
		err = errors.New("timeout")
		return
	}

	return
}

func (p *Scheduler) doCurrJobsAndNext() {
	jobs := p.GetCurrJobWraps()
	p.doJobs(jobs)

	atomic.AddInt32(&p.CurrIndex, 1)
	if p.CurrIndex == 3600 {
		p.CurrIndex = 0
		log.Info("runner", "runed 1 hour ", time.Now())
		// 应该一个小时调用一次
	}
}

func (p *Scheduler) doJobs(jobWraps *[]*JobWrap) {
	if len(*jobWraps) == 0 {
		return
	}

	jobWrapsTemp := (*jobWraps)[:0]

	for i := range *jobWraps {
		jobWrap := (*jobWraps)[i]
		if jobWrap.Deep == 0 {
			go func(jobWrap *JobWrap) {
				jobWrap.Count++
				err := p.doJob(jobWrap)
				if err != nil {
					// retry 4 times
					if jobWrap.Count <= len(p.config.Retry) {
						//var sleepTime int64 = 1
						sleepTime := int64(p.config.Retry[jobWrap.Count-1])
						log.Warn("runner", "job will retry (%v) after %ds(%dth), err :%v", jobWrap.job, sleepTime, jobWrap.Count, err)

						p.rollbackJobWrap(sleepTime, jobWrap)
					} else {
						log.Warn("runner", "job fail (%v), err: %v", jobWrap.job, err)
						// 持久化
						if p.config.Persistence {
							err := deleteJob(jobWrap.IdDb)
							if err != nil {
								log.Error("runner-pers", err)
							}
						}
					}
				} else {
					log.Info("runner", "job success (%v)", jobWrap.job)
					// 持久化
					if p.config.Persistence {
						err := deleteJob(jobWrap.IdDb)
						if err != nil {
							log.Error("runner-pers", err)
						}
					}
				}
			}(jobWrap)
		} else {
			jobWrap.Deep--
			jobWrapsTemp = append(jobWrapsTemp, jobWrap)
		}
	}

	*jobWraps = jobWrapsTemp

}

func (p *Scheduler) Info() string {
	d, _ := json.Marshal(p)
	return string(d)
}
