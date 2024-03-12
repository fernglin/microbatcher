package samples

import (
	"fmt"
	"time"

	"github.com/fernglin/microbatcher"
)

type Job struct {
	ID int
}

type JobResult struct {
	ID     int
	Result string
}

type BatchProcessor struct{}

func (p *BatchProcessor) Process(batch []microbatcher.Job) microbatcher.JobResult {
	results := make(microbatcher.JobResult, len(batch))
	for i, j := range batch {
		job := j.(Job)
		time.Sleep(20 * time.Millisecond)
		results[i] = JobResult{
			ID:     job.ID,
			Result: fmt.Sprintf("Processed job %d in batch", job.ID),
		}
	}
	return results
}

//lint:ignore U1000 Supressing "unused function" as this is a sample
func simpleUsage() {
	config := microbatcher.Config{
		BatchSize:     5,
		BatchInterval: time.Millisecond * 500,
		ShowBatchInfo: false,
	}

	processor := &BatchProcessor{}
	b, err := microbatcher.NewMicroBatcher(config, processor)
	if err != nil {
		fmt.Println("Error:", err)
	}

	b.Start()

	// Start a goroutine to receive and process batch results
	resultChan := b.GetBatchResults()
	go func() {
		for batchResult := range resultChan {
			fmt.Printf("Batch result: %v\n", batchResult)
		}
	}()

	for i := 0; i < 25; i++ {
		job := Job{ID: i}
		b.SubmitJob(job)
	}

	time.Sleep(2 * time.Second)

	b.Shutdown()
}
