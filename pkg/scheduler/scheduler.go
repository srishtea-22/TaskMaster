package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/srishtea-22/TaskMaster/pkg/common"
)

type CommandRequest struct {
	Command		 string `json:"command"`
	ScheduledAt  string `json:"scheduled_at"`
}

type Task struct {
	Command  	string
	ScheduledAt int64
}

type SchedulerServer struct {
	serverPort			string
	dbConnectionString  string
	dbPool				*pgxpool.Pool
	ctx					context.Context
	cancel				context.CancelFunc
	httpServer			*http.Server
}

func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort: 		port,
		dbConnectionString: dbConnectionString,
		ctx: 				ctx,
		cancel: 			cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.ConnectToDB(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	http.HandleFunc("/schedule", s.handlePostTask)
	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}

	log.Printf("Starting scheduler server on %s\n", s.serverPort)

	go func ()  {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %s\n", err)
		}	
	}()

	return s.awaitShutdown()
}

func (s *SchedulerServer) handlePostTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var commandReq CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received schedule request: %+v", commandReq)

	scheduledTime, err := time.Parse(time.RFC3339, commandReq.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid date format. Use ISO 8601 format.", http.StatusBadRequest)
		return
	}

	unixTimestamp := scheduledTime.Unix()

	taskId, err := s.insertTaskIntoDB(context.Background(), Task{Command: commandReq.Command, ScheduledAt: unixTimestamp})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	response := fmt.Sprintf("Command: %s\n Scheduled at (unix timestamp): %d\n Task Id: %s", commandReq, unixTimestamp, taskId)

	w.Write([]byte(response))
}

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	scheduledTime := time.Unix(task.ScheduledAt, 0)
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES $1 $2 RETURNING id"
	
	var insertedId string

	err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, scheduledTime).Scan(&insertedId)

	if err != nil {
		return "", err
	}

	log.Printf("Inserted task %s into DB successfully", insertedId)
	return insertedId, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	s.dbPool.Close()

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}

	log.Println("Scheduler server and database pool stopped.")
	return nil
}