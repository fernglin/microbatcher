package microbatcher

import (
	"time"
)

// Incoming job to be sent for processing
type Job struct {
	Data interface{}
}

type MicroBatcher struct {
	batchSize        int           // Maximum number of jobs per batch
	batchFreq        time.Duration // Maximum time.Duration before a batch is sent for processing
	jobsCh           chan Job      // Channel for incoming jobs
	funcProcessBatch func([]Job)   // Function to process batches when ready
	shutdownCh       chan struct{} // Channel to recieve shutdown signal
}

// Instantiate a new MicroBatcher with a specific batch size and frequency.
func NewMicroBatcher(batchSize int, batchFreq time.Duration, funcProcessBatch func([]Job)) *MicroBatcher {
	return &MicroBatcher{
		batchSize:        batchSize,
		batchFreq:        batchFreq,
		jobsCh:           make(chan Job),
		shutdownCh:       make(chan struct{}),
		funcProcessBatch: funcProcessBatch,
	}
}

// Submits a job to the MicroBatcher.
func (mb *MicroBatcher) SubmitJob(job Job) {
	mb.jobsCh <- job
}

// Starts the MicroBatcher
func (mb *MicroBatcher) Start() {
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
		// Shutdown signal received. Send job for processing.
		case <-mb.shutdownCh:
			if len(batch) > 0 {
				mb.processBatch(batch)
			}
			return
		}
	}

}

// Send job for processing by BatchProcessor as a callback
func (mb *MicroBatcher) processBatch(batch []Job) {
	mb.funcProcessBatch(batch)
}

// Method to shut down the MicroBatcher
func (mb *MicroBatcher) Shutdown() {
	close(mb.shutdownCh)
}
