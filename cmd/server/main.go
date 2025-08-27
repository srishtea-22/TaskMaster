package main

import (
	"flag"

	"github.com/srishtea-22/TaskMaster/pkg/server"
)

var serverPort = flag.String("server_port", ":50050", "Port on which coordinator serves requests.")

func main() {
	flag.Parse()

	coordinator := server.NewServer(*serverPort)
	coordinator.Start()
}
