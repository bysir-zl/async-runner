package core

import (
	"github.com/bysir-zl/bygo/cache"
	"github.com/bysir-zl/bygo/log"
	"github.com/bysir-zl/bygo/util/uuid"
	"time"
)

var redis = cache.NewRedis("")

func readAllUndoJobs(newJob NewJobFunc) (jobWraps *[]*JobWrap, err error) {
	unDoJobs, err := redis.HGETALL(tableNameUndoJob)
	if err != nil {
		return
	}

	jobWraps_ := make([]*JobWrap, len(unDoJobs))

	index := 0
	for id, data := range unDoJobs {
		jw := &JobWrap{
			job:  newJob(),
			IdDb: id,
		}
		err = jw.Unmarshal(data.([]byte))
		if err != nil {
			return
		}

		jobWraps_[index] = jw
		index++
	}

	jobWraps = &jobWraps_
	return
}

func addJob(jobWrap *JobWrap) (err error) {
	data, err := jobWrap.Marshal()
	if err != nil {
		return
	}
	id := uuid.Rand().Hex()
	jobWrap.IdDb = id

	err = redis.HMSET(tableNameUndoJob, id, data, 0)
	if err != nil {
		return
	}

	return
}

func deleteJobs(jobIds []string) (aff int64, err error) {
	err = redis.HDEL(tableNameUndoJob, jobIds...)
	if err != nil {
		return
	}
	return
}

var willDeleteJobIds chan string = make(chan string, 2000)


func deleteJob(jobId string) (err error) {
	willDeleteJobIds <- jobId
	return
}

func InitPersistence(redisHost string) {
	redis = cache.NewRedis(redisHost)

	go func() {
		ids := []string{}
		for {
			select {
			case jobId := <-willDeleteJobIds:
				ids = append(ids, jobId)
			case <-time.Tick(time.Second * 5):
				if len(ids) == 0 {
					continue
				}
				_, err := deleteJobs(ids)
				if err != nil {
					log.Error("runner-pers", err)
				}
				ids = []string{}
			}
		}
	}()
}
