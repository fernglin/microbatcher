package models

// Incoming job to be sent for processing
type Job struct {
	Data interface{}
}

// Job returned from the BatchProcessor
type JobReturn struct {
	Data interface{}
}
