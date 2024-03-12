# A Basic Golang Micro-batching Library

Micro-batching is a technique used in data processing where small amounts of data are collected over a short period and processed together as a batch. It improves efficiency by reducing overhead associated with processing individual items, often used in streaming systems for more optimized throughput and resource utilization.

This is a basic library that allows you to configure a micro-batcher with a specific batch size and batch interval and subsequently hook up your own BatchProcessor.

## Built with
- [Go 1.22](https://go.dev/)

## Usage
### Implement structs for `Job` and `JobResult`

```go
type Job struct {
	ID int
}

type JobResult struct {
	ID     int
	Result string
}
```

### Implement a custom `BatchProcessor` for your jobs that has a main method `Process`

```go
type BatchProcessor struct{}

func (bp *BatchProcessor) Process(jobs []microbatcher.Job) microbatcher.JobResult {
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
```

### Instantiate and use the Micro-batcher

```go
config := microbatcher.Config{
    BatchSize:     5,
    BatchInterval: time.Millisecond * 500,
    ShowBatchInfo: true,
}

batchProcessor := &BatchProcessor{}

// Instantiate new MicroBatcher
batcher, err := microbatcher.NewMicroBatcher(config, batchProcessor)
if err != nil {
    fmt.Println("Error:", err)
}

// Start the MicroBatcher
batcher.Start()
```

### Sending Jobs to the Batcher

```go
// Using example job struct above
job := Job{ID: i}
batcher.SubmitJob(job)
```

### Use a Goroutine to receive JobResults
```go
resultCh := batcher.GetBatchResults()
go func() {
    for batchResult := range resultCh {
        fmt.Printf("Batch result: %v\n", batchResult)
    }
}()
```

### Shutting down the Batcher

```go
batcher.Shutdown()
```

## Sample
See detailed sample in the `samples` folder