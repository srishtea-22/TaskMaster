package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
	"github.com/srishtea-22/TaskMaster/pkg/common"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
)

const (
	defaultMaxMisses = 1
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	grpcSever          *grpc.Server
	listener           net.Listener
	WorkerPool         map[uint32]*workerInfo
	WorkerPoolMutex    sync.RWMutex
	maxHeartbeatMisses uint8
	heartbeatInterval  time.Duration
	roundRobinIndex    uint32
	TaskStatus         map[string]pb.TaskStatus
	taskStatusMutex    sync.RWMutex
	serverPort         string
	ctx				   context.Context
	cancel 			   context.CancelFunc
	wg 				   sync.WaitGroup
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func NewServer(port string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*workerInfo),
		TaskStatus:         make(map[string]pb.TaskStatus),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartbeat,
		serverPort:         port,
		ctx: 				ctx,
		cancel: 			cancel,
	}
}

func (s *CoordinatorServer) Start() error {
	go s.manageWorkerPool()

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server failed to start: %w", err)
	}

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", s.serverPort)

	if err != nil {
		return err
	}

	log.Printf("Starting grpc server on %s", s.serverPort)
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
	s.cancel()
	s.wg.Wait()

	s.WorkerPoolMutex.Lock()
	for _, worker := range s.WorkerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}
	s.WorkerPoolMutex.Unlock()

	if s.grpcSever != nil {
		s.grpcSever.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	return nil
}

func (s *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	data := in.GetData()
	taskId := uuid.New().String()

	task := &pb.TaskRequest{
		TaskId: taskId,
		Data:   data,
	}

	if err := s.submitTaskToWorker(task); err != nil {
		return nil, err
	}

	s.taskStatusMutex.Lock()
	defer s.taskStatusMutex.Unlock()

	s.TaskStatus[taskId] = pb.TaskStatus_QUEUED

	return &pb.ClientTaskResponse{
		Message: "Task submitted successfully",
		TaskId:  taskId,
	}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	// placeholder implementation
	status := req.GetStatus()
	taskId := req.TaskId

	s.taskStatusMutex.Lock()
	defer s.taskStatusMutex.Unlock()

	s.TaskStatus[taskId] = status

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.WorkerPoolMutex.RLock()
	defer s.WorkerPoolMutex.RUnlock()

	workerCount := len(s.WorkerPool)
	if workerCount == 0 {
		return nil
	}

	keys := make([]uint32, 0, workerCount)
	for k := range s.WorkerPool {
		keys = append(keys, k)
	}
	key := keys[s.roundRobinIndex%uint32(workerCount)]
	s.roundRobinIndex++
	return s.WorkerPool[key]
}

func (s *CoordinatorServer) GetTaskStatus(ctx context.Context, in *pb.GetTaskStatusRequest) (*pb.GetTaskStatusResponse, error) {
	taskId := in.GetTaskId()

	s.taskStatusMutex.Lock()
	defer s.taskStatusMutex.Unlock()

	return &pb.GetTaskStatusResponse{
		TaskId: taskId,
		Status: s.TaskStatus[taskId],
	}, nil
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	worker_id := in.GetWorkerId()
	//log.Println("Recieved heartbeat from worker: ", worker_id)

	if worker, ok := s.WorkerPool[worker_id]; ok {
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker: ", worker_id)
		conn, err := grpc.NewClient(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.WorkerPool[worker_id] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		log.Println("Registered worker: ", worker_id)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case<-ticker.C:
			s.removeInactiveWorkers()
		case<-s.ctx.Done():
			return
		}
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for worker_id, worker := range s.WorkerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", worker_id)
			worker.grpcConnection.Close()
			delete(s.WorkerPool, worker_id)
		} else {
			worker.heartbeatMisses++
		}
	}
}
