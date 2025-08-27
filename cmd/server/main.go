package main

import (
	"fmt"

	"github.com/srishtea-22/TaskMaster/pkg/server"
)

func main() {
	srv := server.NewServer(":50050")
	fmt.Println("Starting server")
	srv.Start()
}
