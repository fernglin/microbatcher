package microbatcher_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fernglin/microbatcher"
)

// Mock type for returned results
type ProcessedResults struct {
	JobDone int
	Success bool
}

func TestMicroBatcher(t *testing.T) {
	batchSize := 5
	batchFreq := time.Millisecond * 500

	// Instantiate new MicroBatcher
	batcher := microbatcher.NewMicroBatcher(batchSize, batchFreq, mockBatchProcessor)
	resultsCh := batcher.ResultsChannel()

	// Start the MicroBatcher
	batcher.Start()

	// Submit jobs
	for i := 0; i < 50; i++ {
		job := microbatcher.Job{Data: i}
		err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
	}

	// Wait to allow for processing
	time.Sleep(time.Millisecond * 1000)

	// Shutdown the MicroBatcher
	batcher.Shutdown()

	for result := range resultsCh {
		fmt.Println("Received result:", result)
	}

	// Attempt to submit more jobs
	for i := 500; i < 650; i++ {
		job := microbatcher.Job{Data: i}
		batcher.SubmitJob(job)
		err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
	}
}

// Mock BatchProcessor for Testing
// Processor will print processed data when received from MicroBatcher
func mockBatchProcessor(batch []microbatcher.Job) interface{} {
	jobsDone := 0
	// Process the batch of jobs
	for _, job := range batch {
		// Add some delay to emulate processing
		time.Sleep(time.Millisecond * 10)
		fmt.Println("Processed data:", job.Data)
		jobsDone++
	}
	// Print when batch is processed
	fmt.Println("--Batch Break--")
	return ProcessedResults{JobDone: jobsDone, Success: true}
}
