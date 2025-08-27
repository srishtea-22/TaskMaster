package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/srishtea-22/TaskMaster/pkg/common"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	coordinatorAddress = "localhost:50050"
	taskProcessTime    = 5 * time.Second
	workerPoolSize     = 10
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	listener                 net.Listener
	grpcServer               *grpc.Server
	id                       uint32
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	serverPort               string
	taskQueue                chan *pb.TaskRequest
}

func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Recieved task: %s", req.GetData())

	go processTask(req.GetData())
	return &pb.TaskResponse{Message: "Task was submitted", Success: true, TaskId: req.TaskId}, nil
}

func processTask(data string) {
	log.Printf("Processing Task: %s", data)
	time.Sleep(taskProcessTime)
	log.Println("Completed Task", data)
}

func (w *WorkerServer) Start() error {
	w.startWorkerPool(workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	return w.startGRPCServer()
}

func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for range numWorkers {
		go w.worker()
	}
}

func (w *WorkerServer) worker() {
	for task := range w.taskQueue {
		processTask(task.GetData())
	}
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	conn, err := grpc.NewClient(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)
	log.Println("Connected to coordinator")
	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	ticker := time.NewTicker(w.heartbeatInterval)

	for range ticker.C {
		if err := w.sendHeartbeat(); err != nil {
			log.Printf("Failed to send heartbeat: %v", err)
			break
		}
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  w.listener.Addr().String(),
	})
	return err
}

func (w *WorkerServer) startGRPCServer() error {
	var err error
	w.listener, err = net.Listen("tcp", w.serverPort)

	if err != nil {
		return fmt.Errorf("failed to listen to %s: %v", w.serverPort, err)
	}

	log.Printf("Starting worker server at %s", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	return w.grpcServer.Serve(w.listener)
}

func (w *WorkerServer) Stop() error {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
		w.listener = nil
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			return fmt.Errorf("failed to close the listener: %w", err)
		}
	}

	log.Printf("Worker server at %d stopped", w.id)
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}
}

func NewServer(port string) *WorkerServer {
	return &WorkerServer{
		id:                uuid.New().ID(),
		serverPort:        port,
		heartbeatInterval: common.DefaultHeartbeat,
		taskQueue:         make(chan *pb.TaskRequest, 100),
	}
}
