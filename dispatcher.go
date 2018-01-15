package main

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

var wg sync.WaitGroup

// Dispatcher dispatches jobs
type Dispatcher struct {
	Workers []*Worker
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool     chan chan Job
	maxWorkers     int
	wg             sync.WaitGroup
	stopping       bool
	dispatchedJobs int
	processedJobs  int
}

// NewDispatcher returns a new Dispatcher
func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		Workers:        []*Worker{},
		WorkerPool:     pool,
		wg:             sync.WaitGroup{},
		maxWorkers:     maxWorkers,
		stopping:       false,
		dispatchedJobs: 0,
		processedJobs:  0,
	}
}

// Run Runs the dispatcher
func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, &d.processedJobs)
		d.Workers = append(d.Workers, &worker)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {

	for {
		if d.stopping {
			return
		}
		select {
		case job := <-JobQueue:
			wg.Add(1)
			d.dispatchedJobs++
			// a job request has been received
			go func(job Job) {
				defer wg.Done()
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				log.Debug("Waiting for Worker...")
				jobChannel := <-d.WorkerPool
				log.Debug("Got Worker...")

				// dispatch the job to the worker job channel
				log.Debug("Dispatching Job...")
				jobChannel <- job
				log.Debug("Done dispatching Job...")
			}(job)
		}
	}
}

// Stop all workers.
func (d *Dispatcher) Stop() {
	d.stopping = true
	wg.Wait()
	log.Infof("Dispatched %d jobs", d.dispatchedJobs)
	log.Infof("Processed %d jobs", d.processedJobs)
	log.Debug("Length of workers:", len(d.Workers))
	for i := len(d.Workers) - 1; i >= 0; i-- {
		log.Debug("Stopping worker:", i)
		d.Workers[i].Stop()
	}
	close(JobQueue)
	close(d.WorkerPool)
}
