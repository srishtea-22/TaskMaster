package worker

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"google.golang.org/grpc"
)

type WorkerServer struct {
	listener     net.Listener
	workerServer *grpc.Server
}

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SubmitTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Recieved %v", in.GetData())

	go s.processTask(in.GetData())
	return &pb.TaskResponse{Message: "Task was submitted", Success: true, TaskId: in.TaskId}, nil
}

func (s *workerServiceServer) processTask(data string) {
	log.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	log.Println("Completed Task", data)
}

func (w *WorkerServer) Start() error {
	PORT := ":50051"
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Printf("Failed to listen: %v", err)
		return err
	}

	log.Println("Started server at", PORT)
	w.listener = lis
	w.workerServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.workerServer, &workerServiceServer{})
	if err := w.workerServer.Serve(lis); err != nil {
		log.Printf("Failed to serve: %v", err)
		return err
	}
	return nil
}

func (w *WorkerServer) Stop() error {
	w.workerServer.GracefulStop()

	if err := w.listener.Close(); err != nil {
		log.Printf("Failed to close listener %v", err)
		return err
	}

	log.Println("Worker server stopped")
	return nil
}

func NewServer() *WorkerServer {
	return &WorkerServer{}
}
