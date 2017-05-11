package core

type Job interface {
	Run() error
	String() string
	Unmarshal([]byte) error // 反序列化
	Marshal() ([]byte, error) // 序列化
	Unique() []byte // 必须返回一个[]byte,将做为唯一标示,相同唯一标示的job会替换已存在的job
}

// 包裹JOB
type JobWrap struct {
	job   Job

	Id    int64
	Deep  int64 // 圈数，一圈1小时
	Count int64 // 第几次运行
}
