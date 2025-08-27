package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
)

const (
	serverPort       = ":50050"
	httpServerPort   = ":8080"
	shutdownTimeout  = 5 * time.Second
	defaultMaxMisses = 2
	defaultHeartbeat = 5
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
	grpcSever           *grpc.Server
	httpServer          *http.Server
	listener            net.Listener
	workerPool          map[uint32]*workerInfo
	mutex               sync.RWMutex
	maxHeartbeatMisses  uint8
	heartbeatInterval   uint8
	roundRobinIndex     uint32
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func NewServer() *CoordinatorServer {
	return &CoordinatorServer{
		workerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  defaultHeartbeat,
	}
}

func (s *CoordinatorServer) Start() error {
	go s.manageWorkerPool()

	if err := s.startHTTPserver(); err != nil {
		return fmt.Errorf("HTTP server failed to start: %w", err)
	}

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server failed to start: %w", err)
	}

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startHTTPserver() error {
	s.httpServer = &http.Server{Addr: httpServerPort, Handler: http.HandlerFunc(s.handleTaskRequest)}

	go func() {
		log.Printf("Starting HTTP server at %s", httpServerPort)
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", serverPort)

	if err != nil {
		return err
	}

	log.Printf("Starting grpc server on %s", serverPort)
	s.grpcSever = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcSever, s)

	go func() {
		if err := s.grpcSever.Serve(s.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	return s.Stop()
}

func (s *CoordinatorServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
	}

	s.mutex.Lock()
	for _, worker := range s.workerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}
	s.mutex.Unlock()

	if s.grpcSever != nil {
		s.grpcSever.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	return nil
}

func (s *CoordinatorServer) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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

	if err := s.submitTask(task); err != nil {
		http.Error(w, fmt.Sprintf("Task submission failed: %s", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Task submitted")
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	workerCount := len(s.workerPool)
	if workerCount == 0 {
		return nil
	}

	keys := make([]uint32, 0, workerCount)
	for k := range s.workerPool {
		keys = append(keys, k)
	}
	key := keys[s.roundRobinIndex%uint32(workerCount)]
	s.roundRobinIndex++
	return s.workerPool[key]
}

func (s *CoordinatorServer) submitTask(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	worker_id := in.GetWorkerId()
	log.Println("Recieved heartbeat from worker: ", worker_id)

	if worker, ok := s.workerPool[worker_id]; ok {
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker: ", worker_id)
		conn, err := grpc.NewClient(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.workerPool[worker_id] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		log.Println("Registered worker: ", worker_id)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) manageWorkerPool() {
	ticker := time.NewTicker(time.Duration(s.heartbeatInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.removeInactiveWorkers()
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for worker_id, worker := range s.workerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", worker_id)
			delete(s.workerPool, worker_id)
			worker.grpcConnection.Close()
		} else {
			worker.heartbeatMisses++
		}
	}
}
