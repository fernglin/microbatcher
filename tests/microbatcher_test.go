package microbatcher_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fernglin/microbatcher"
	"github.com/stretchr/testify/assert"
)

type Job struct {
	ID int
}

type JobResult struct {
	ID     int
	Result string
}

type MockBatchProcessor struct{}

func (bp *MockBatchProcessor) Process(jobs []microbatcher.Job) microbatcher.JobResult {
	results := make(microbatcher.JobResult, len(jobs))
	for i, j := range jobs {
		job := j.(Job)
		time.Sleep(20 * time.Millisecond)
		results[i] = JobResult{
			ID:     job.ID,
			Result: fmt.Sprintf("Processed job %d in batch", job.ID),
		}
	}
	return results
}

func TestMicroBatcher(t *testing.T) {
	jobCompleteCount := 0
	config := microbatcher.Config{
		BatchSize:     5,
		BatchInterval: time.Millisecond * 500,
		ShowBatchInfo: true,
	}

	batchProcessor := &MockBatchProcessor{}

	// Instantiate new MicroBatcher
	batcher, err := microbatcher.NewMicroBatcher(config, batchProcessor)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Start the MicroBatcher
	batcher.Start()

	// Start a goroutine to receive and process batch results
	resultCh := batcher.GetBatchResults()
	go func() {
		for batchResult := range resultCh {
			fmt.Printf("Batch result: %v\n", batchResult)
			jobCompleteCount++
		}
	}()

	// Submit jobs
	for i := 0; i < 25; i++ {
		job := Job{ID: i}
		err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		// Assert no errors when submitting jobs normally
		assert.NoError(t, err)
	}

	time.Sleep(2 * time.Second)

	// Shutdown the MicroBatcher
	batcher.Shutdown()

	// Attempt to submit more jobs
	for i := 1010; i < 1011; i++ {
		job := Job{ID: i}
		err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		// Assert error received that microbatcher has shut down
		assert.Error(t, err)
	}

	// Expect 5 batches (of 5 jobs) to be completed
	assert.Equal(t, 5, jobCompleteCount)
}
