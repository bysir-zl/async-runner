package async_runner

import (
	"github.com/bysir-zl/bygo/log"
	"time"
)

type Scheduler struct {
	Tasks     [3600]Task // 3600个task 每一秒需要执行一个
	CurrIndex int32      // 当前正在执行哪个task
}

type Task struct {
	Jobs []*Job // 当前task里的任务
}

func NewScheduler() *Scheduler {
	s := &Scheduler{}
	for i := 0; i < 3600; i++ {
		s.Tasks[i].Jobs = []*Job{}
	}
	return s
}

// 开启工作循环
func (p *Scheduler) Work() {
	preTime := time.Now()
	for {
		nowTime := time.Now()
		jobs := p.Tasks[p.CurrIndex].Jobs
		go p.doJobs(jobs)

		time.Sleep(preTime.Add(time.Second).Sub(nowTime))
		preTime = preTime.Add(time.Second)

		p.CurrIndex++
		if p.CurrIndex == 3600 {
			p.CurrIndex = 0
			log.Info("runner", "runed 1 hour")
			// 应该是上一次时间的一个小时后
			// todo 可能会延后,应当修复时间
		}
	}
}

func (p *Scheduler) doJobs(jobs []*Job) {
	if len(jobs) == 0 {
		return
	}

	for _, job := range jobs {
		if job.Deep == 0 {
			go func() {
				err := (*job).Run()
				if err != nil {
					// todo retry logic
				}
			}()
		} else {
			job.Deep--
		}
	}
}

// 秒为单位
func (p *Scheduler) addJob(duration int64, fun func() error) {
	deep := duration / 3600
	index := int32(duration%3600) + p.CurrIndex

	job := &Job{
		Deep: deep,
		Run:  fun,
	}
	if duration == 0 {
		p.doJobs([]*Job{job})
		return
	}

	p.Tasks[index].Jobs = append(p.Tasks[index].Jobs, job)
}
