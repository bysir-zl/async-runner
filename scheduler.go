package async_runner

import "time"

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
		go p.doJobs()
		nowTime := time.Now()
		<-time.After(nowTime.Sub(preTime.Add(time.Second)))
		preTime = nowTime
		p.CurrIndex++
		if p.CurrIndex == 3600-1 {
			p.CurrIndex = 0
			// 应该是上一次时间的一个小时后
			// todo 可能会延后,应当修复时间
		}
	}
}

func (p *Scheduler) doJobs() {
	jobs := p.Tasks[p.CurrIndex].Jobs
	if jobs != nil || len(jobs) == 0 {
		return
	}

	for _, job := range jobs {
		if job.Deep == 0 {
			go func() {
				err := (*job).Run()
				if err != nil {
					// todo logger
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

	p.Tasks[index].Jobs = append(p.Tasks[index].Jobs, &Job{
		Deep: deep,
		Run:  fun,
	})
}
