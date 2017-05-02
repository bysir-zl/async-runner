package async_runner

import (
	"github.com/bysir-zl/bygo/log"
	"testing"
)

func TestServer(t *testing.T) {
	s := NewScheduler()
	s.addJob(1, func() error {
		log.Info("test", "doing job:1")
		return nil
	})
	s.addJob(3, func() error {
		log.Info("test", "doing job:3")
		return nil
	})

	s.addJob(7, func() error {
		log.Info("test", "doing job:7")
		return nil
	})

	go s.Work()

	s.addJob(5, func() error {
		log.Info("test", "doing job:5")
		return nil
	})

	<-(chan int)(nil)
}