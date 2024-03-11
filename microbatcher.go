package microbatcher

import (
	"errors"
	"sync"
	"time"
)

// Incoming job to be sent for processing
type Job struct {
	Data interface{}
}

type MicroBatcher struct {
	batchSize        int                     // Maximum number of jobs per batch
	batchFreq        time.Duration           // Maximum time.Duration before a batch is sent for processing
	jobsCh           chan Job                // Channel for incoming jobs
	funcProcessBatch func([]Job) interface{} // Function to process batches when ready
	resultsCh        chan interface{}        // Channel for results from BatchProcessor
	shutdownCh       chan struct{}           // Channel to recieve shutdown signal
	waitGroup        sync.WaitGroup
}

// Instantiate a new MicroBatcher with a specific batch size and frequency.
func NewMicroBatcher(batchSize int, batchFreq time.Duration, funcProcessBatch func([]Job) interface{}) *MicroBatcher {
	return &MicroBatcher{
		batchSize:        batchSize,
		batchFreq:        batchFreq,
		jobsCh:           make(chan Job),
		shutdownCh:       make(chan struct{}),
		funcProcessBatch: funcProcessBatch,
		resultsCh:        make(chan interface{}),
	}
}

// Submits a job to the MicroBatcher.
func (mb *MicroBatcher) SubmitJob(job Job) error {
	select {
	case <-mb.shutdownCh:
		return errors.New("MicroBatcher has been shut down")
	case mb.jobsCh <- job:
		return nil
	}

}

// Starts the MicroBatcher
func (mb *MicroBatcher) Start() {
	// Add waitGroup counter (i.e. Wait for 1 goroutine)
	mb.waitGroup.Add(1)
	go func() {
		// Decrease waitGroup counter when goroutine completes. Defers execution of this.
		defer mb.waitGroup.Done()
		var batch []Job
		var batchTimer *time.Timer
		for {
			select {
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
			// Shutdown signal received. Send job for processing then shut down MicroBatcher gracefully.
			case <-mb.shutdownCh:
				if len(batch) > 0 {
					mb.processBatch(batch)
					// Wait for all tasks to complete
					mb.waitGroup.Wait()
					close(mb.resultsCh)
				}
				return
			}
		}
	}()
}

// Send job for processing by BatchProcessor as a callback
func (mb *MicroBatcher) processBatch(batch []Job) {
	results := mb.funcProcessBatch(batch)
	mb.resultsCh <- results
}

// Method to retreive results from the BatchProcessor
func (mb *MicroBatcher) ResultsChannel() <-chan interface{} {
	return mb.resultsCh
}

// Method to shut down the MicroBatcher
func (mb *MicroBatcher) Shutdown() {
	close(mb.shutdownCh)
	// Wait for all tasks to complete
	mb.waitGroup.Wait()
}
