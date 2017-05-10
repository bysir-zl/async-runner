package core

type Job interface {
	Run() error
	String() string
}

// 包裹JOB
type JobWrap struct {
	job Job

	Id    int64
	Deep  int64 // 圈数，一圈1小时
	Count int64 // 第几次运行
}
