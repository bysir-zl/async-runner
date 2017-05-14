package core

import (
	"github.com/bysir-zl/bygo/log"
	"time"
	"bytes"
	"github.com/bysir-zl/orm"
)

type Scheduler struct {
	Tasks      [3600]Task       // 3600个task 每一秒需要执行一个
	CurrIndex  int32            // 当前正在执行哪个task
	config     *SchedulerConfig // 是否使用持久化
	newJobFunc NewJobFunc
}

type Task struct {
	JobWraps []*JobWrap // 当前task里的任务
}

type SchedulerConfig struct {
	Persistence bool  `json:"persistence"` // 是否使用redis持久化
	Redis       string `json:"redis"`      // redis
	Log         bool `json:"log"`          // 是否记录日志
	MysqlLink   string `json:"mysql"`      // mysql
}

type NewJobFunc func() Job

func NewScheduler(c *SchedulerConfig, newJobFunc NewJobFunc) *Scheduler {
	s := &Scheduler{
		config:     c,
		newJobFunc: newJobFunc,
	}
	for i := 0; i < 3600; i++ {
		s.Tasks[i].JobWraps = []*JobWrap{}
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
		d := time.Unix(jobWrap.RunTime, 0).Sub(now)
		p.rollbackJobWrap(int64(d), jobWrap)
	}

	return
}

// 开启工作循环
func (p *Scheduler) Work() {
	preTime := time.Now()
	d := time.Second
	for {
		nowTime := time.Now()
		jobs := &p.Tasks[p.CurrIndex].JobWraps

		go p.doJobs(jobs)

		time.Sleep(preTime.Add(d).Sub(nowTime))
		preTime = preTime.Add(d)

		p.CurrIndex++
		if p.CurrIndex == 3600 {
			p.CurrIndex = 0
			log.Info("runner", "runed 1 hour")
			// 应该是上一次时间的一个小时后
			// todo 可能会延后,应当修复时间
		}
	}
}

// 删除所有相同job
func (p *Scheduler) DeleteJob(job Job) (ok bool) {
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
	index := int32(duration%3600) + p.CurrIndex
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

	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, jobWrap)

	// 持久化
	if p.config.Persistence {
		jobWrap.RunTime = time.Now().Add(time.Duration(duration)).Unix()
		err := addJob(jobWrap)
		if err != nil {
			log.Error("runner-pers", err)
		}
	}
}

func (p *Scheduler) doJobs(jobWraps *[]*JobWrap) {
	if len(*jobWraps) == 0 {
		return
	}
	lock.Lock()
	defer lock.Unlock()

	deletedCount := 0
	for i := range *jobWraps {
		jobWrap := (*jobWraps)[i-deletedCount]
		if jobWrap.Deep == 0 {
			go func(jobWrap *JobWrap) {
				jobWrap.Count++
				err := jobWrap.job.Run()
				if err != nil {
					// retry 4 times
					if jobWrap.Count != 2 {
						sleepTime := jobWrap.Count*4 - 1
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

			// remove job
			*jobWraps = append((*jobWraps)[:i-deletedCount], (*jobWraps)[i+1-deletedCount:]...)
			deletedCount++
		} else {
			jobWrap.Deep--
		}
	}

}
var testRollCount = 0
// 秒为单位
func (p *Scheduler) rollbackJobWrap(duration int64, jobWrap *JobWrap) {
	testRollCount++
	if duration < 0 {
		duration = 0
	}
	deep := duration / 3600
	index := int32(duration%3600) + p.CurrIndex
	if index >= 3600 {
		index = index % 3600
		deep ++
	}

	jobWrap.Deep = deep
	if duration == 0 {
		p.doJobs(&[]*JobWrap{jobWrap})
		return
	}

	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, jobWrap)
}
