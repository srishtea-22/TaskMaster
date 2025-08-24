package worker

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"google.golang.org/grpc"
)

type workerServer struct {
	listener     net.Listener
	workerServer *grpc.Server
}

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SendTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Recieved %v", in.GetData())

	s.processTask(in.GetData())
	return &pb.TaskResponse{Result: "Task processed"}, nil
}

func (s *workerServiceServer) processTask(data string) {
	log.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	log.Println("Completed Task", data)
}

func (w *workerServer) Start() {
	PORT := ":50051"
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Started server at", PORT)
	w.listener = lis
	w.workerServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.workerServer, &workerServiceServer{})
	if err := w.workerServer.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}
}

func NewServer() workerServer {
	return workerServer{}
}
