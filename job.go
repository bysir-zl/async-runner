package async_runner

type Job struct {
	Deep int64 // 圈数，一圈1小时

	Run func() error
}
