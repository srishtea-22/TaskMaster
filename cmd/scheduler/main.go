package main

import (
	"flag"
	"log"

	"github.com/srishtea-22/TaskMaster/pkg/common"
	"github.com/srishtea-22/TaskMaster/pkg/scheduler"
)


var schedulerPort = flag.String("scheduler_port", ":8081", "Port on which scheduler serves requests.")

func main() {
	dbConnectionString := common.GetDBConnectionString()
	schedulerService := scheduler.NewServer(*schedulerPort, dbConnectionString)

	err := schedulerService.Start()
	if err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}