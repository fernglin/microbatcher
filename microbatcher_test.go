package microbatcher_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fernglin/microbatcher"
	"github.com/stretchr/testify/assert"
)

func TestMicroBatcher(t *testing.T) {
	batchSize := 5
	batchFreq := time.Millisecond * 500

	// Instantiate new MicroBatcher
	batcher := microbatcher.NewMicroBatcher(batchSize, batchFreq, mockBatchProcessor)

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
		assert.NoError(t, err)
	}

	// Wait to allow for processing
	time.Sleep(time.Millisecond * 500)

	// Shutdown the MicroBatcher
	batcher.Shutdown()

	// Attempt to submit more jobs
	for i := 500; i < 650; i++ {
		job := microbatcher.Job{Data: i}
		batcher.SubmitJob(job)
		err := batcher.SubmitJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
		assert.Error(t, err)
	}
}

// Mock BatchProcessor for Testing
// Processor will print processed data when received from MicroBatcher
func mockBatchProcessor(batch []microbatcher.Job) {
	// Process the batch of jobs
	for _, job := range batch {
		// Add some delay to emulate processing
		time.Sleep(time.Millisecond * 10)
		fmt.Println("Processed data:", job.Data)
	}
	// Print when batch is processed
	fmt.Println("--Batch Break--")
}
