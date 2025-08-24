package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
)

type server struct {
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func (s *server) sendTaskToWorker(data string) {
	_, err := s.workerServiceClient.SendTask(context.Background(), &pb.TaskRequest{Data: data})
	if err != nil {
		log.Fatalf("could not send task: %v", err)
	}
}

func (s *server) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
		}
		go s.sendTaskToWorker(string(body))
		fmt.Fprintf(w, "Task sent to worker")
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func (s *server) connectToWorker() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}

	s.grpcConnection = conn
	s.workerServiceClient = pb.NewWorkerServiceClient(s.grpcConnection)
}

func (s *server) Start() {
	log.Println("Connecting to worker...")
	s.connectToWorker()
	defer s.grpcConnection.Close()
	log.Println("Connected to worker!")

	http.HandleFunc("/", s.handleTaskRequest)
	log.Println("Starting front end server at :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (s *server) Stop() {
}

func NewServer() server {
	return server{}
}
