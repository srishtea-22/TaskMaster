package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"google.golang.org/grpc"
)

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SendTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Recieved %v", in.GetData())

	go s.processTask(in.GetData())
	return &pb.TaskResponse{Result: "Task recieved and is being processed"}, nil
}

func (s *workerServiceServer) processTask(data string) {
	fmt.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	fmt.Println("Completed Task", data)
}

func Start() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &workerServiceServer{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}
}
