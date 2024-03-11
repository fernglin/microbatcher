package microbatcher_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fernglin/microbatcher"
	"github.com/stretchr/testify/assert"
)

// Mock type for returned results
type ProcessedResults struct {
	JobDone int
	Success bool
}

type MockBatchProcessor struct{}

func (bp *MockBatchProcessor) Process(jobs []microbatcher.Job) []microbatcher.JobResult {
	results := make([]microbatcher.JobResult, len(jobs))
	for i, job := range jobs {
		// Perform processing on the job
		fmt.Println("Processing job:", job)
		results[i] = "Result for job " + fmt.Sprint(job)
	}
	return results
}

func TestMicroBatcher(t *testing.T) {
	batchSize := 5
	batchFreq := time.Millisecond * 500

	processor := &MockBatchProcessor{}

	// Instantiate new MicroBatcher
	batcher, err := microbatcher.NewMicroBatcher(batchSize, batchFreq, processor)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Start the MicroBatcher
	batcher.Start()

	// Submit jobs
	for i := 0; i < 15; i++ {
		job := i
		result, err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println("Job result:", result)
		assert.NoError(t, err)
	}

	// Wait to allow for processing
	time.Sleep(time.Millisecond * 1000)

	// Shutdown the MicroBatcher
	batcher.Shutdown()

	// Attempt to submit more jobs
	for i := 500; i < 650; i++ {
		job := i
		batcher.SubmitJob(job)
		result, err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println("Job result:", result)
		assert.Error(t, err)
	}
}
