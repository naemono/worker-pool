package dispatcher

import (
	"sync"

	"github.com/naemono/worker-pool/worker"

	log "github.com/sirupsen/logrus"
)

// Dispatcher dispatches jobs
type Dispatcher struct {
	Workers []*worker.Worker
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool     chan chan worker.Job
	jobQueue       chan worker.Job
	maxWorkers     int
	wg             sync.WaitGroup
	dispatchedJobs int
	processedJobs  int
	workerFunc     func(job worker.Job) error
}

// NewDispatcher returns a new Dispatcher
func NewDispatcher(maxWorkers int, workerFunc func(job worker.Job) error) *Dispatcher {
	pool := make(chan chan worker.Job, maxWorkers)
	jobQueue := make(chan worker.Job, maxWorkers)
	return &Dispatcher{
		Workers:        []*worker.Worker{},
		WorkerPool:     pool,
		wg:             sync.WaitGroup{},
		maxWorkers:     maxWorkers,
		jobQueue:       jobQueue,
		dispatchedJobs: 0,
		processedJobs:  0,
		workerFunc:     workerFunc,
	}
}

// Run Runs the dispatcher
func (d *Dispatcher) Run() {
	// starting n number of workers
	log.Infof("Dispatcher: starting %d workers...", d.maxWorkers)
	for i := 0; i < d.maxWorkers; i++ {
		worker := worker.NewWorker(d.WorkerPool, &d.processedJobs, d.workerFunc)
		d.Workers = append(d.Workers, &worker)
		worker.Start()
	}

	go d.dispatch()
}

// Stop all workers.
func (d *Dispatcher) Stop() {
	d.wg.Wait()
	log.Infof("Dispatched %d jobs", d.dispatchedJobs)
	log.Infof("Processed %d jobs", d.processedJobs)
	log.Debugf("Length of workers: %d", len(d.Workers))
	for i := len(d.Workers) - 1; i >= 0; i-- {
		log.Debugf("Stopping worker: %d", i)
		d.Workers[i].Stop()
		log.Debugf("Stopped worker: %d", i)
	}
}

// SendJob sends job into jobQueue
func (d *Dispatcher) SendJob(job worker.Job) {
	log.Debug("about to send job in SendJob..")
	d.jobQueue <- job
	log.Debug("After send job in SendJob...")
	return
}

func (d *Dispatcher) dispatch() {

	log.Debug("in dispatch method...")
	for {
		select {
		case job := <-d.jobQueue:
			log.Debug("Got job in dispatch method...")
			d.dispatchedJobs++
			// a job request has been received
			go func(job worker.Job) {
				d.wg.Add(1)
				defer func() {
					d.wg.Done()
				}()
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
