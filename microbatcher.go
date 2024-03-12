package microbatcher

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Incoming job to be sent for processing
type Job interface{}

// Result of processing a batch of Jobs
type JobResult []interface{}

type BatchProcessor interface {
	Process(batch []Job) JobResult
}

type Config struct {
	BatchSize     int           // Max number of jobs per batch
	BatchInterval time.Duration // Max interval before a batch is sent for processing
	ShowBatchInfo bool          // Show information about batches processed (for testing)
}

type MicroBatcher struct {
	Config         Config
	batchProcessor BatchProcessor // Hook for BatchProcessor
	jobsCh         chan Job       // Channel for incoming jobs
	batchCh        chan []Job     // Channel for sending job batches to process
	resultsCh      chan JobResult // Channel for results from BatchProcessor
	shutdownCh     chan struct{}  // Channel to receive shutdown signal
	wg             sync.WaitGroup
}

// Instantiate a new MicroBatcher with a specific batch size and frequency.
func NewMicroBatcher(config Config, batchProcessor BatchProcessor) (*MicroBatcher, error) {
	if config.BatchSize <= 0 {
		return nil, errors.New("batch size must be a positive integer")
	}
	if config.BatchInterval <= 0 {
		return nil, errors.New("batch interval must be a positive time.Duration")
	}

	return &MicroBatcher{
		Config:         config,
		batchProcessor: batchProcessor,
		jobsCh:         make(chan Job),
		batchCh:        make(chan []Job),
		resultsCh:      make(chan JobResult),
		shutdownCh:     make(chan struct{}),
	}, nil
}

// Submits a job to the MicroBatcher.
func (mb *MicroBatcher) SubmitJob(job Job) error {
	select {
	case <-mb.shutdownCh:
		return errors.New("microBatcher has been shut down")
	case mb.jobsCh <- job:
		return nil
	}
}

// Returns a channel to receive results of processed batches
func (mb *MicroBatcher) GetBatchResults() <-chan JobResult {
	return mb.resultsCh
}

// Starts the MicroBatcher
func (mb *MicroBatcher) Start() {
	go mb.startBatchingWorker()
	mb.startProcessWorker()
}

// Worker that watches the jobs channels and timer
func (mb *MicroBatcher) startBatchingWorker() {
	batch := make([]Job, 0, mb.Config.BatchSize)
	timer := time.NewTimer(mb.Config.BatchInterval)
	defer timer.Stop()

	for {
		select {
		case <-mb.shutdownCh:
			// If microbatch is shutting down & batch > 0, send reminaing jobs to batch channel
			job := <-mb.jobsCh
			batch = append(batch, job)
			if len(batch) > 0 {
				mb.batchCh <- batch
			}
			return
		// When jobs are received on the jobs channel
		case job := <-mb.jobsCh:
			batch = append(batch, job)
			// If batch size is reached, send batch slice to batch channel
			if len(batch) == cap(batch) {
				mb.batchCh <- batch
				batch = make([]Job, 0, mb.Config.BatchSize)
				timer.Reset(mb.Config.BatchInterval)
			}
		// If batch interval is reached & batch > 0, send jobs to batch channel
		case <-timer.C:
			if len(batch) > 0 {
				mb.batchCh <- batch
				batch = make([]Job, 0, mb.Config.BatchSize)
			}
			timer.Reset(mb.Config.BatchInterval)
		}
	}
}

// Worker that watches the batch channel and sends job batches for processing
func (mb *MicroBatcher) startProcessWorker() {
	mb.wg.Add(1)
	go func() {
		defer mb.wg.Done()
		for batch := range mb.batchCh {
			results := mb.batchProcessor.Process(batch)
			// Send processed results to results channel
			mb.resultsCh <- results
			if mb.Config.ShowBatchInfo {
				fmt.Printf("Processed batch of %d jobs\n", len(batch))
			}
		}
	}()
}

// Method to shut down the MicroBatcher
func (mb *MicroBatcher) Shutdown() {
	close(mb.shutdownCh)
}
