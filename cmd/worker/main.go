package main

import (
	"flag"

	"github.com/srishtea-22/TaskMaster/pkg/worker"
)

var (
	serverPort      = flag.String("worker_port", "", "Port on which worker serves requests.")
	coordinatorPort = flag.String("coordinator", ":8080", "Network address of coordinator")
)

func main() {
	flag.Parse()

	worker := worker.NewServer(*serverPort, *coordinatorPort)
	worker.Start()
}
