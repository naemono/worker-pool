package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var wfunc func(job Job) error

func init() {
	wfunc = func(job Job) error {
		return nil
	}
}

// TestNewWorker tests the NewWorker func
func TestNewWorker(t *testing.T) {

	pjobs := 0
	pool := make(chan chan Job, 1)
	w := NewWorker(pool, &pjobs, wfunc)

	assert.Equal(t, *w.processedJobs, 0, "processedJobs on init should be 0")
}

// TestStartWorker tests starting the worker
func TestStartWorker(t *testing.T) {

	pjobs := 0
	pool := make(chan chan Job, 1)
	w := NewWorker(pool, &pjobs, wfunc)
	msg := Message{
		Method:    "get",
		ClusterID: "01234",
		Payload:   []byte("test"),
	}
	job := Job{
		Payload: msg,
	}

	w.Start()
	workerJobChannel := <-pool
	workerJobChannel <- job
	w.Stop()
	time.Sleep(1 * time.Second)
	assert.Equal(t, *w.processedJobs, 1, "processedJobs after sending single job should be 1")
}
