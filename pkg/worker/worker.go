package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/srishtea-22/TaskMaster/pkg/common"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
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
	coordinatorAddress       string
}

func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Recieved task: %+v", req)

	w.taskQueue <- req
	return &pb.TaskResponse{Message: "Task was submitted", Success: true, TaskId: req.TaskId}, nil
}

func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing Task: %+v", task)
	time.Sleep(taskProcessTime)
	log.Printf("Completed Task: %+v", task)

	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId: task.GetTaskId(),
		Status: pb.TaskStatus_COMPLETE,
	})
}

func (w *WorkerServer) Start() error {
	ctx := context.Background()
	w.startWorkerPool(ctx, workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat(ctx)

	return w.startGRPCServer()
}

func (w *WorkerServer) startWorkerPool(ctx context.Context, numWorkers int) {
	for range numWorkers {
		go w.worker(ctx)
	}
}

func (w *WorkerServer) worker(ctx context.Context) {
	for {
		select {
		case task := <-w.taskQueue:
			w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
				TaskId: task.GetTaskId(),
				Status: pb.TaskStatus_PROCESSING,
			})
			w.processTask(task)
		case<-ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	conn, err := grpc.NewClient(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)
	log.Println("Connected to coordinator")
	return nil
}

func (w *WorkerServer) periodicHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case<-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		case<-ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
	}
	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.serverPort == "" {
		w.listener, err = net.Listen("tcp", ":0")
		w.serverPort = fmt.Sprintf("%d", w.listener.Addr().(*net.TCPAddr).Port)
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen to %s: %v", w.serverPort, err)
	}

	log.Printf("Starting worker server at %s", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	return w.grpcServer.Serve(w.listener)
}

func (w *WorkerServer) Stop() error {
	w.closeGRPCConnection()

	log.Printf("Worker server at %d stopped", w.id)
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}
}

func NewServer(port string, coordinator string) *WorkerServer {
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		heartbeatInterval:  common.DefaultHeartbeat,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		coordinatorAddress: coordinator,
	}
}
