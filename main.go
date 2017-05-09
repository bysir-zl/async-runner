package main

import "github.com/bysir-zl/async-runner/server"

func main() {
	s := server.NewServer()
	s.Start()
}
