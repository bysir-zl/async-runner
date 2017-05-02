package async_runner

type Job struct {
	Deep int64 // 圈数，一圈1小时
	Count int64 // 第几次运行

	Run func() error
}
