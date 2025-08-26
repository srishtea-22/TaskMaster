package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
)

type Server struct {
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
	httpServer          *http.Server
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start() error {
	if err := s.setUpGRPCConnection(); err != nil {
		return err
	}

	defer s.grpcConnection.Close()

	if err := s.startHTTPserver(); err != nil {
		return err
	}

	return s.awaitShutdown()
}

func (s *Server) setUpGRPCConnection() error {
	log.Println("Connecting to worker...")
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return fmt.Errorf("failed to connect with worker %w", err)
	}

	s.grpcConnection = conn
	s.workerServiceClient = pb.NewWorkerServiceClient(s.grpcConnection)
	log.Println("Connected to worker!")
	return nil
}

func (s *Server) startHTTPserver() error {
	s.httpServer = &http.Server{Addr: ":8080", Handler: nil}
	http.HandleFunc("/", s.handleTaskRequest)

	errChan := make(chan error, 1)
	go func() {
		log.Println("Starting HTTP server at :8080")
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			errChan <- fmt.Errorf("failed to start HTTP server: %w", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func (s *Server) awaitShutdown() error {
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	return s.shutdownHTTPserver()
}

func (s *Server) shutdownHTTPserver() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to gracefully shutfdown server %w", err)
	}
	return nil
}

func (s *Server) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method request", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	task := &pb.TaskRequest{
		TaskId: uuid.New().String(),
		Data:   string(body),
	}

	if err = s.submitTask(task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Task submitted")
}

func (s *Server) submitTask(task *pb.TaskRequest) error {
	_, err := s.workerServiceClient.SubmitTask(context.Background(), task)

	if err != nil {
		return fmt.Errorf("error submitting task: %w", err)
	}

	return nil
}

func (s *Server) Stop() error {
	if s.httpServer != nil {
		if err := s.shutdownHTTPserver(); err != nil {
			return err
		}
	}

	if s.grpcConnection != nil {
		s.grpcConnection.Close()
	}

	return nil
}
