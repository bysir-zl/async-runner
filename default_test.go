package async_runner

import (
	"github.com/bysir-zl/bygo/log"
	"testing"
)

func TestServer(t *testing.T) {
	s:=NewScheduler()
	s.addJob(1, func() error {
		log.Info("test")
		return nil
	})

	s.Work()

}