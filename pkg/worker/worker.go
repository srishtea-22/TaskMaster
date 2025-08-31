package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	coordinatorConnection    *grpc.ClientConn
	ctx						 context.Context
	cancel 					 context.CancelFunc
	wg 						 sync.WaitGroup
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
	w.startWorkerPool(workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return w.awaitShutdown()
}

func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for range numWorkers {
		w.wg.Add(1)
		go w.worker()
	}
}

func (w *WorkerServer) worker() {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.taskQueue:
			w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
				TaskId: task.GetTaskId(),
				Status: pb.TaskStatus_PROCESSING,
			})
			w.processTask(task)
		case<-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	var err error
	w.coordinatorConnection, err = grpc.NewClient(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorConnection)
	log.Println("Connected to coordinator")
	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case<-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		case<-w.ctx.Done():
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

	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (w *WorkerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return w.Stop()
}

func (w *WorkerServer) Stop() error {
	w.cancel()
	w.wg.Wait()

	w.closeGRPCConnection()

	log.Printf("Worker server at %d stopped", w.id)
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Failed to close listener: %v", err)
		}
	}

	if err := w.coordinatorConnection.Close(); err != nil {
		log.Printf("Error while closing client connection to coordinator: %v", err)
	}
}

func NewServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		heartbeatInterval:  common.DefaultHeartbeat,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		coordinatorAddress: coordinator,
		ctx: 				ctx,
		cancel: 			cancel,
	}
}
