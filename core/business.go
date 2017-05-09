package core

// transfer2go core data transfer module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
)

// Policy defines request policy
type Policy struct {
	Name string
}

// WorkQueueElement is an element which we operate in WorkQueue
type WorkQueueElement struct {
	RequestName string
	Status      string
	StartPolicy Policy
}

// Job represents the job to be run with given request
type Job struct {
	Request Record
}

// Worker represents the worker that executes the job
type Worker struct {
	Id         int
	JobPool    chan chan Job
	JobChannel chan Job
	quit       chan bool
}

// Dispatcher implementation
type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	JobPool    chan chan Job
	MaxWorkers int
}

// JobQueue is a buffered channel that we can send work requests on.
var JobQueue chan Job

// NewWorker return a new instance of the Worker type
func NewWorker(wid int, jobPool chan chan Job) Worker {
	return Worker{
		Id:         wid,
		JobPool:    jobPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.JobPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:

				// Increment number of running jobs
				WorkqueueMetrics.Jobs.Inc(1)

				//                 log.Println("job", job)

				// perform some work with a job
				rec := parseRequest(job.Request)
				fmt.Println("### Request", rec)

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

// NewDispatcher returns new instance of Dispatcher type
func NewDispatcher(maxWorkers, maxQueue int, mfile string, minterval int64) *Dispatcher {
	// register metrics
	f, e := os.OpenFile(mfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		log.Fatalf("error opening file: %v", e)
	}
	defer f.Close()

	// define agent's metrics
	r := metrics.DefaultRegistry
	jobs := metrics.GetOrRegisterCounter("jobs", r)
	WorkqueueMetrics = Metrics{Jobs: jobs}
	go metrics.Log(r, time.Duration(minterval)*time.Second, log.New(f, "metrics: ", log.Lmicroseconds))

	// define pool of workers and jobqueue
	pool := make(chan chan Job, maxWorkers)
	JobQueue = make(chan Job, maxQueue)
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers}
}

// Run function starts the worker and dispatch it as go-routine
func (d *Dispatcher) Run(rtype string, interval int64) {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		worker.Start()
	}
	// spawn new go-routine to dispatch
	go d.dispatch(rtype, interval)
}

func (d *Dispatcher) dispatch(rtype string, interval int64) {
	for {
		// fetch new set of requests from ReqMgr2
		requests := GetRequests(rtype)
		for _, req := range requests {
			// submit request to processing chain
			go func(req Record) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.JobPool
				// dispatch the request to the worker job channel
				job := Job{Request: req}
				jobChannel <- job
			}(req)
		}
		time.Sleep(time.Duration(interval) * time.Second) // wait for a job
	}
}
