package main

import (
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	dispatch "github.com/naemono/test-concurrency/dispatcher"
	"github.com/naemono/test-concurrency/worker"
)

var (
	// MaxWorker is the max num of workers
	MaxWorker = os.Getenv("MAX_WORKERS")
	// MaxQueue is the max num of queues
	MaxQueue = os.Getenv("MAX_QUEUE")
)

// // Message is the message type to be received
// type Message struct {
// 	Method    string `json: "Method"`
// 	ClusterID string `json: "ClusterID,omitempty"`
// 	// Intent here is to ensure json, or yaml
// 	// Example here https://mlafeldt.github.io/blog/teaching-go-programs-to-love-json-and-yaml/
// 	Payload []byte `json: "Payload"`
// }
//
// // Job represents the job to be run
// type Job struct {
// 	Payload Message
// }

func init() {
	if strings.Compare(MaxWorker, "") == 0 {
		MaxWorker = "10"
	}
	if strings.Compare(MaxQueue, "") == 0 {
		MaxQueue = "2"
	}
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

func main() {
	var maxWorkers int
	var err error

	if maxWorkers, err = strconv.Atoi(MaxWorker); err != nil {
		log.Error("Error converting %s to Digit: %s", MaxWorker, err.Error())
	}
	log.Info("Starting Program")
	dispatch.JobQueue = make(chan worker.Job, maxWorkers)
	dispatcher := dispatch.NewDispatcher(maxWorkers)
	dispatcher.Run()

	defer dispatcher.Stop()

	for i := 0; i < 5000000; i++ {
		// let's create a job with the payload
		msg := worker.Message{
			Method:    "POST",
			ClusterID: "01234",
			Payload:   []byte("test"),
		}
		work := worker.Job{Payload: msg}

		// Push the work onto the queue.
		dispatch.JobQueue <- work
	}
	time.Sleep(5 * time.Second)
}
