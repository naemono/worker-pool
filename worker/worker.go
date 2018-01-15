package worker

import (
	log "github.com/sirupsen/logrus"
)

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool    chan chan Job
	JobChannel    chan Job
	quit          chan bool
	processedJobs *int
}

// Message is the message type to be received
type Message struct {
	Method    string `json: "Method"`
	ClusterID string `json: "ClusterID,omitempty"`
	// Intent here is to ensure json, or yaml
	// Example here https://mlafeldt.github.io/blog/teaching-go-programs-to-love-json-and-yaml/
	Payload []byte `json: "Payload"`
}

// Job represents the job to be run
type Job struct {
	Payload Message
}

// NewWorker return s new Worker
func NewWorker(workerPool chan chan Job, processedJobs *int) Worker {
	return Worker{
		WorkerPool:    workerPool,
		JobChannel:    make(chan Job, 4),
		quit:          make(chan bool),
		processedJobs: processedJobs,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				log.Debug("Got job", job.Payload.Method)
				*w.processedJobs++

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
