package dispatcher

import (
	"testing"
	"time"

	"github.com/naemono/worker-pool/worker"
	"github.com/stretchr/testify/assert"
)

var wfunc func(job worker.Job) error

func init() {
	wfunc = func(job worker.Job) error {
		return nil
	}
}

// TestNewWorker tests the NewWorker func
func TestNewDispatcher(t *testing.T) {
	d := NewDispatcher(1, wfunc)

	assert.Equal(t, d.maxWorkers, 1, "maxWorkers on init should be 1")
	assert.Equal(t, d.dispatchedJobs, 0, "dispatchedJob on init should be 0")
	assert.Equal(t, d.processedJobs, 0, "processedJobs on init should be 0")
}

func TestRunDispatcher(t *testing.T) {
	d := NewDispatcher(1, wfunc)
	d.Run()

	assert.Len(t, d.Workers, 1, "Number of Workers on init should be 1")
}

func TestDispatcherAddJob(t *testing.T) {
	d := NewDispatcher(1, wfunc)
	d.Run()

	msg := worker.Message{
		Method:    "POST",
		ClusterID: "01234",
		Payload:   []byte("test"),
	}
	work := worker.Job{Payload: msg}

	// Push the work onto the queue.
	d.SendJob(work)

	time.Sleep(1 * time.Second)
	d.Stop()
	assert.Equal(t, d.dispatchedJobs, 1)
}
