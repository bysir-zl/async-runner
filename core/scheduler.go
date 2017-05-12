package core

import (
	"github.com/bysir-zl/bygo/log"
	"time"
)

type Scheduler struct {
	Tasks     [3600]Task // 3600个task 每一秒需要执行一个
	CurrIndex int32      // 当前正在执行哪个task
}

type Task struct {
	JobWraps []*JobWrap // 当前task里的任务
}

func NewScheduler() *Scheduler {
	s := &Scheduler{}
	for i := 0; i < 3600; i++ {
		s.Tasks[i].JobWraps = []*JobWrap{}
	}
	return s
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

func (p *Scheduler) doJobs(jobWarps *[]*JobWrap) {
	if len(*jobWarps) == 0 {
		return
	}

	deletedCount := 0
	for i, jobWrap := range *jobWarps {
		if jobWrap.Deep == 0 {
			go func(jobWrap JobWrap) {
				jobWrap.Count++
				err := jobWrap.job.Run()
				if err != nil {
					// retry 4 times
					if jobWrap.Count != 5 {
						sleepTime := jobWrap.Count * 4 - 1
						log.Warn("runner", "job will retry (%v) after %ds(%dth), err :%v", jobWrap.job, sleepTime, jobWrap.Count, err)

						p.addJobWrap(sleepTime, jobWrap)
					} else {
						log.Warn("runner", "job fail (%v), err: %v", jobWrap.job, err)
					}
				} else {
					log.Info("runner", "job success (%v)", jobWrap.job)
				}
			}(*jobWrap)

			// remove job
			*jobWarps = append((*jobWarps)[:i - deletedCount], (*jobWarps)[i + 1 - deletedCount:]...)
			deletedCount++
		} else {
			(*jobWarps)[i].Deep--
		}
	}

}

// 秒为单位
func (p *Scheduler) AddJob(duration int64, job Job) {
	deep := duration / 3600
	index := int32(duration % 3600) + p.CurrIndex
	if index >= 3600 {
		index = index % 3600
		deep ++
	}

	jobWrap := &JobWrap{Deep: deep, job: job}

	log.Info("runner", "job added: (%v), duration: %ds", job,duration)

	if duration == 0 {
		p.doJobs(&[]*JobWrap{jobWrap})
		return
	}

	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, jobWrap)
}

// 秒为单位
func (p *Scheduler) addJobWrap(duration int64, jobWrap JobWrap) {
	deep := duration / 3600
	index := int32(duration % 3600) + p.CurrIndex
	if index >= 3600 {
		index = index % 3600
		deep ++
	}

	jobWrap.Deep = deep
	if duration == 0 {
		p.doJobs(&[]*JobWrap{&jobWrap})
		return
	}

	p.Tasks[index].JobWraps = append(p.Tasks[index].JobWraps, &jobWrap)
}
