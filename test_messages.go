package main

import (
	"github.com/naemono/worker-pool/worker"
	log "github.com/sirupsen/logrus"
)

func sendTestMessages() {
	for i := 0; i < 5000000; i++ {
		// let's create a job with the payload
		msg := worker.Message{
			Method:    "POST",
			ClusterID: "01234",
			Payload:   []byte("test"),
		}
		work := worker.Job{Payload: msg}

		log.Debugf("Sending message: %d", i)
		// Push the work onto the queue.
		Dispatcher.SendJob(work)
	}
}

func sendMessage(job worker.Job) error {
	log.Debugf("Got message %s", job.Payload.Method)
	return nil
}
