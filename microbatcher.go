package microbatcher

import (
	"errors"
	"sync"
	"time"
)

// Incoming job to be sent for processing
type Job interface{}

type JobResult interface{}

type BatchProcessor interface {
	Process(jobs []Job) []JobResult
}

type MicroBatcher struct {
	batchSize  int            // Maximum number of jobs per batch
	batchFreq  time.Duration  // Maximum time.Duration before a batch is sent for processing
	processor  BatchProcessor // Function to process batches when ready
	jobsCh     chan Job       // Channel for incoming jobs
	resultsCh  chan JobResult // Channel for results from BatchProcessor
	shutdownCh chan struct{}  // Channel to recieve shutdown signal
	wg         sync.WaitGroup
}

// Instantiate a new MicroBatcher with a specific batch size and frequency.
func NewMicroBatcher(batchSize int, batchFreq time.Duration, processor BatchProcessor) (*MicroBatcher, error) {
	if batchSize <= 0 {
		return nil, errors.New("batch size must be a positive integer")
	}
	if batchFreq <= 0 {
		return nil, errors.New("batch frequency must be a positive time.Duration")
	}

	return &MicroBatcher{
		batchSize:  batchSize,
		batchFreq:  batchFreq,
		processor:  processor,
		jobsCh:     make(chan Job),
		resultsCh:  make(chan JobResult),
		shutdownCh: make(chan struct{}),
	}, nil
}

// Submits a job to the MicroBatcher.
func (mb *MicroBatcher) SubmitJob(job Job) (JobResult, error) {
	select {
	case <-mb.shutdownCh:
		return nil, errors.New("MicroBatcher has been shut down")
	case mb.jobsCh <- job:
		results := <-mb.resultsCh
		return results, nil
	}
}

// Starts the MicroBatcher
func (mb *MicroBatcher) Start() {
	// Add waitGroup counter (i.e. Wait for 1 goroutine)
	mb.wg.Add(1)
	go func() {
		// Decrease waitGroup counter when goroutine completes. Defers execution of this.
		defer mb.wg.Done()
		var batch []Job
		var batchTimer *time.Timer
		for {
			select {
			// Shutdown signal received. Send job for processing then shut down MicroBatcher gracefully.
			case <-mb.shutdownCh:
				if len(batch) > 0 {
					mb.processBatch(batch)
					// Wait for all tasks to complete
					mb.wg.Wait()
					close(mb.resultsCh)
				}
				return
			// Jobs coming into jobs channel
			case job := <-mb.jobsCh:
				batch = append(batch, job)
				// Send job for processing once batch size limit is reached
				if len(batch) >= mb.batchSize {
					mb.processBatch(batch)
					batch = nil
					// Reset batchTimer once batch is processed
					if batchTimer != nil {
						batchTimer.Stop()
						batchTimer = nil
					}
					// If batchTimer is nil, start a new timer with duration defined in batchFrequency
				} else if batchTimer == nil {
					batchTimer = time.NewTimer(mb.batchFreq)
					go func() {
						// Process the batch if timer has completed and no further jobs are received
						<-batchTimer.C
						mb.processBatch(batch)
						batch = nil
					}()
				}
			}
		}
	}()
}

// Send job for processing by BatchProcessor as a callback
func (mb *MicroBatcher) processBatch(batch []Job) {
	results := mb.processor.Process(batch)
	for _, result := range results {
		mb.resultsCh <- result
	}

}

// Method to shut down the MicroBatcher
func (mb *MicroBatcher) Shutdown() {
	close(mb.shutdownCh)
	// Wait for all tasks to complete
	mb.wg.Wait()
}
