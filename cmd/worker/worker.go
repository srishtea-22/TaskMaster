package main

import (
	"github.com/srishtea-22/TaskMaster/pkg/worker"
)

func main() {
	workerServer := worker.NewServer(":50051")
	workerServer.Start()
}
