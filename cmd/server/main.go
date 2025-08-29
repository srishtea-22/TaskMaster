package main

import (
	"flag"

	"github.com/srishtea-22/TaskMaster/pkg/server"
)

var serverPort = flag.String("server_port", ":8080", "Port on which coordinator serves requests.")

func main() {
	flag.Parse()

	coordinator := server.NewServer(*serverPort)
	coordinator.Start()
}
