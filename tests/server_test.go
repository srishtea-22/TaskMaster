package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/srishtea-22/TaskMaster/pkg/common"
	pb "github.com/srishtea-22/TaskMaster/pkg/grpcapi"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
)

var cluster Cluster
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func setup(numWorkers int8) {
	cluster = Cluster{}
	cluster.LaunchCluster(":50050", numWorkers)
	conn, client = CreateTestClient("localhost:50050")
}

func teardown() {
	cluster.StopCluster()
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestE2ESuccess(t *testing.T) {
	setup(2)
	defer teardown()

	assertion := assert.New(t)

	submitResponse, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}
	taskId := submitResponse.GetTaskId()

	statusResponse, err := client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	if err != nil {
		t.Fatalf("failed to get task status: %v", err)
	}
	assertion.Equal(pb.TaskStatus_PROCESSING, statusResponse.GetStatus())

	err = WaitForCondition(func() bool {
		statusResponse, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
		if err != nil {
			log.Printf("Failed to get task status: %v", err)
		}
		return statusResponse.GetStatus() == pb.TaskStatus_COMPLETE
	}, 10 * time.Second, 500 * time.Millisecond)

	
	if err != nil {
		t.Fatalf("task did not complete within the timeout: %v", err)
	}
}

func TestWorkersNotAvailable(t *testing.T) {
	setup(2)
	defer teardown()

	for _, worker := range cluster.workers {
		worker.Stop()
	}

	err := WaitForCondition(func() bool {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		return err != nil && err.Error() == "rpc error: code = Unknown desc = no workers available"
	}, 20 * time.Second, common.DefaultHeartbeat)

	if err != nil {
		t.Fatalf("Coordinator did not clean up the workers within SLO. Error: %s", err.Error() )
	}
}

func TestTaskLoadBalancingOverWorkers(t *testing.T) {
	setup(4)
	defer teardown()

	for i := 0; i < 8; i++ {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	err := WaitForCondition(func() bool {
		for _, worker := range cluster.workers {
			worker.ReceivedTasksMutex.Lock()
			if len(worker.ReceivedTasks) != 2 {
				worker.ReceivedTasksMutex.Unlock()
				return false
			}
			worker.ReceivedTasksMutex.Unlock()
		}
		return true
	}, 5*time.Second, 500*time.Millisecond)

	if err != nil {
		for idx, worker := range cluster.workers {
			worker.ReceivedTasksMutex.Lock()
			log.Printf("Worker %d has %d tasks in its log", idx, len(worker.ReceivedTasks))
		
			worker.ReceivedTasksMutex.Unlock()
		}
		t.Fatalf("Coordinator is not using round-robin to execute tasks over worker pool.")
	}
}