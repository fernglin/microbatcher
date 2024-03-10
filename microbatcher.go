package microbatcher

import (
	"time"

	"github.com/fernglin/microbatcher/models"
)

type MicroBatcher struct {
	batchSize  int             // Maximum number of jobs per batch
	batchFreq  time.Duration   // Maximum time.Duration before a batch is sent for processing
	jobsCh     chan models.Job // Channel for incoming jobs
	shutdownCh chan struct{}   // Channel to recieve shutdown signal
}

// Instantiate a new MicroBatcher with a specific batch size and frequency.
func NewMicroBatcher(batchSize int, batchFreq time.Duration) *MicroBatcher {
	return &MicroBatcher{
		batchSize:  batchSize,
		batchFreq:  batchFreq,
		jobsCh:     make(chan models.Job),
		shutdownCh: make(chan struct{}),
	}
}

// Submits a job to the MicroBatcher.
func (mb *MicroBatcher) SubmitJob(job models.Job) models.JobReturn {
	returnCh := make(chan models.JobReturn)
	mb.jobsCh <- job
	return <-returnCh
}
