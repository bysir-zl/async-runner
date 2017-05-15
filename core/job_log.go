package core

import (
	"github.com/bysir-zl/orm"
	"time"
)

// job mysql 结构体
// 用于保存成功失败记录
type JobModel struct {
	orm struct{} `table:"job" connect:"default"`

	Id          string    `orm:"col(id)" json:"id"`
	RunTime     int64    `orm:"col(run_time)" json:"run_time"`
	Data        string    `orm:"col(data)" json:"data"`
	SuccessTime int64    `orm:"col(success_time)" json:"success_time"`
	FailTime    int64    `orm:"col(fail_time)" json:"fail_time"`
	Error       string `orm:"col(error)" json:"error"`
}

func readAllNotSuccessJobDb(newJob NewJobFunc) (jobWraps *[]*JobWrap, err error) {
	jobsModel := []JobModel{}
	has, err := orm.Model(jobsModel).Where("success_time = 0 AND fail_time = 0").Select(&jobsModel)
	if err != nil {
		return
	}
	if !has {
		return
	}
	jobWraps_ := make([]*JobWrap, len(jobsModel))
	for i, job := range jobsModel {
		j := newJob()
		j.Unmarshal([]byte(job.Data))
		jobWraps_[i] = &JobWrap{
			RunTime: job.RunTime,
			job:     j,
			IdDb:    job.Id,
		}
	}

	jobWraps = &jobWraps_

	return
}

func addJobLogSuccess(jobId string) (aff int64, err error) {
	now := time.Now().Unix()
	update := map[string]interface{}{
		"success_time": now,
	}
	aff, err = orm.Model((*JobModel)(nil)).
		Where("id = ?", jobId).
		Update(update)
	if err != nil {
		return
	}
	return
}

func addJobLogFail(jobId string, error string) (aff int64, err error) {
	now := time.Now().Unix()
	update := map[string]interface{}{
		"fail_time": now,
		"error":     error,
	}
	aff, err = orm.Model((*JobModel)(nil)).
		Where("id = ?", jobId).
		Update(update)
	if err != nil {
		return
	}
	return
}

func deleteJobsLog(jobIds []string) (aff int64, err error) {
	if len(jobIds) == 0 {
		return
	}
	model := &JobModel{}
	jobIdsDb := make([]interface{}, len(jobIds))
	for i, id := range jobIds {
		jobIdsDb[i] = id
	}
	aff, err = orm.Model(model).WhereIn("id in (?)", jobIdsDb...).Delete()
	if err != nil {
		return
	}
	return
}
