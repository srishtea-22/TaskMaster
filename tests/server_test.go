package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"github.com/srishtea-22/TaskMaster/pkg/server"
	"github.com/srishtea-22/TaskMaster/pkg/worker"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var coordinator *server.CoordinatorServer
var w1 *worker.WorkerServer
var w2 *worker.WorkerServer
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	coordinator = server.NewServer(":50050")
	w1 = worker.NewServer(":50051", "localhost:50050")
	w2 = worker.NewServer(":50052", "localhost:50050")

	startServers()
	createClientConnection()
}

func startServers() {
	startServer(coordinator)
	startServer(w1)
	startServer(w2)

	time.Sleep(10 * time.Second)
}

func startServer(srv interface {
	Start() error
}) {
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("failed to start servers: %v", err)
		}
	}()
}

func createClientConnection() {
	var err error
	conn, err := grpc.NewClient("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatal("could not create test connection to coordinator")
	}
	client = pb.NewCoordinatorServiceClient(conn)

	time.Sleep(5 * time.Second)
}

func teardown() {
	if err := coordinator.Stop(); err != nil {
		log.Printf("Failed to stop server: %v", err)
	}
	if err := w1.Stop(); err != nil {
		log.Printf("Failed to stop worker: %v", err)
	}
	if err := w2.Stop(); err != nil {
		log.Printf("Failed to stop worker: %v", err)
	}
}

func TestServerIntegration(t *testing.T) {
	t.Parallel()

	assertion := assert.New(t)

	submitResponse, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}
	taskId := submitResponse.GetTaskId()

	statusResponse, err := client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	if err != nil {
		log.Fatalf("failed to get task status: %v", err)
	}
	assertion.Equal(pb.TaskStatus_PROCESSING, statusResponse.GetStatus())

	time.Sleep(7 * time.Second)

	statusResponse, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	if err != nil {
		log.Fatalf("failed to get task status: %v", err)
	}
	assertion.Equal(pb.TaskStatus_COMPLETE, statusResponse.GetStatus())
}
