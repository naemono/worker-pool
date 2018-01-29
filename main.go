package main

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	dispatch "github.com/naemono/worker-pool/dispatcher"
)

var (
	// MaxWorker is the max num of workers
	MaxWorker = os.Getenv("MAX_WORKERS")
	// MaxQueue is the max num of queues
	MaxQueue = os.Getenv("MAX_QUEUE")
	// Dispatcher is the global dispatcher
	Dispatcher *dispatch.Dispatcher
	// LogLevel is the logging level
	LogLevel = os.Getenv("LOGLEVEL")
)

func init() {
	LogLevelMap := map[string]log.Level{
		"INFO":    log.InfoLevel,
		"WARNING": log.WarnLevel,
		"ERROR":   log.ErrorLevel,
		"DEBUG":   log.DebugLevel,
	}
	if strings.Compare(MaxWorker, "") == 0 {
		MaxWorker = "1"
	}
	if strings.Compare(MaxQueue, "") == 0 {
		MaxQueue = "1"
	}
	if strings.Compare(LogLevel, "") == 0 {
		MaxQueue = "INFO"
	}
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	if _, ok := LogLevelMap[LogLevel]; ok {
		log.SetLevel(LogLevelMap[LogLevel])
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

func main() {
	var maxWorkers int
	var err error

	if maxWorkers, err = strconv.Atoi(MaxWorker); err != nil {
		log.Errorf("Error converting %s to Digit: %s", MaxWorker, err.Error())
	}
	Dispatcher = dispatch.NewDispatcher(maxWorkers, sendMessage)
	Dispatcher.Run()

	defer Dispatcher.Stop()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Infof("received Signal: %d", sig)
		done <- true
	}()

	if test := os.Getenv("TEST_MESSAGES"); strings.Compare(test, "true") == 0 {
		sendTestMessages()
	}

	log.Info("Will exit on ctrl-c")
	<-done
	log.Info("exiting")
}
