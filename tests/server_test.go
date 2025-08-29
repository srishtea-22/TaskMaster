package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
)

var cluster Cluster
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	cluster = Cluster{}
	cluster.LaunchCluster(":50050", 2)
	conn, client = CreateTestClient("localhost:50050")
}

func teardown() {
	cluster.StopCluster()
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

	err = WaitForCondition(func() bool {
		statusResponse, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
		if err != nil {
			log.Printf("Failed to get task status: %v", err)
		}
		return statusResponse.GetStatus() == pb.TaskStatus_COMPLETE
	}, 10 * time.Second)

	
	if err != nil {
		log.Fatalf("task did not complete within the timeout: %v", err)
	}
}
