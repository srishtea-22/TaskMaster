package main

import (
	"flag"

	"github.com/srishtea-22/TaskMaster/pkg/coordinator"
)

var coordinatorPort = flag.String("coordinator_port", ":8080", "Port on which coordinator serves requests.")

func main() {
	flag.Parse()

	coordinator := coordinator.NewServer(*coordinatorPort)
	coordinator.Start()
}
