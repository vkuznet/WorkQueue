package core

// transfer2go core data transfer module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"log" // keep standard log here since we used it in metrics, do not use logrus
	"os"
	"strings"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"github.com/vkuznet/WorkQueue/services"
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// Job represents the job to be run with given request
type Job struct {
	Request utils.Record
	Type    string
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
				// perform some work with a job
				if job.Type == "process" {
					Process(job.Request)
				} else if job.Type == "cleanup" {
					Cleanup(job.Request)
				} else {
					logrus.Warn("Unsupported job type: %s", job.Type)
				}
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
func (d *Dispatcher) Run(rtype string, interval, cleanup int64) {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		worker.Start()
	}
	// spawn new go-routine to fetch requests from ReqMgr2 and process them
	go d.dispatch(rtype, interval)
	// spawn new go-routine to clean-up requests in WorkQueue
	go d.cleanup(cleanup)
}

// helper function to dispatch jobs from ReqMgr2
func (d *Dispatcher) dispatch(rtype string, interval int64) {
	for {
		// fetch new set of requests from ReqMgr2
		requests := services.GetRequests(rtype)
		for _, req := range requests {
			// submit request to processing chain
			go func(req utils.Record) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.JobPool
				// dispatch the request to the worker job channel
				job := Job{Request: req, Type: "process"}
				jobChannel <- job
			}(req)
		}
		time.Sleep(time.Duration(interval) * time.Second) // wait for a job
	}
}

// helper function to cleanup WorkQueue
func (d *Dispatcher) cleanup(interval int64) {
	for {
		// fetch new set of requests from ReqMgr2
		requests := GetWorkQueueElements("") // here we pass empty key
		for _, req := range requests {
			// submit request to processing chain
			go func(req utils.Record) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.JobPool
				// dispatch the request to the worker job channel
				job := Job{Request: req, Type: "cleanup"}
				jobChannel <- job
			}(req)
		}
		time.Sleep(time.Duration(interval) * time.Second) // wait for a job
	}
}

// Process given request
func Process(record utils.Record) {
	// Increment number of running jobs
	WorkqueueMetrics.Jobs.Inc(1)

	var out []couchdb.CouchDoc
	reqConfig := requestConfig(record)
	rType := requestType(reqConfig)
	switch rType {
	case "MonteCarlo":
		policy := MonteCarloPolicy{Name: "MonteCarlo", Record: record, Config: reqConfig}
		out = policy.Split()
	case "ResubmitBlock":
		policy := ResubmitBlockPolicy{Name: "ResubmitBlock", Record: record, Config: reqConfig}
		out = policy.Split()
	default:
		policy := BlockPolicy{Name: "Block", Record: record, Config: reqConfig}
		out = policy.Split()
	}
	if utils.VERBOSE > 0 {
		fmt.Println("### ReqMgr2 record", record)
		fmt.Println("### WorkQueueElements ###")
		for _, rec := range out {
			fmt.Println(rec)
		}
	}
	// insert WorkQueueElement records into CouchDB
	resp, err := DB.Bulk(out)
	if err != nil {
		logrus.Warn("Insert error: ", err, resp)
	}
	if utils.VERBOSE > 0 {
		logrus.Info("Insert response: ", resp)
	}
}

// helper function to get request config from record name
func requestConfig(record utils.Record) utils.Record {
	var name string
	for rname, _ := range record {
		name = rname
		break
	}
	return services.RequestConfig(name)
}

// helper function which returns request type from given record
func requestType(config utils.Record) string {
	for key, val := range config {
		if strings.Contains(key, "requestType") {
			return val.(string)
		}
	}
	return "Block"
}

// Cleanup performs clean-up of WorkQueue
func Cleanup(record utils.Record) {
	// Decrement number of running jobs
	WorkqueueMetrics.Jobs.Dec(1)
	// NB: here three for loops is actually a one pass, since
	// first for loop gets record name
	// second for loop gets single document from reqMgr2 doc list
	// third gets spec from reqMgr2 document
	for rname, val := range record {
		request := services.GetRequest(rname)
		for _, req := range request { // reqMgr2 returns always a list
			for _, spec := range req { // reqMgr2 record is {request_name: request_spec}
				switch rec := spec.(type) {
				case map[string]interface{}:
					status, _ := rec["RequestStatus"].(string)
					if status == "running-closed" {
						v := val.(map[string]interface{})
						id := v["_id"].(string)
						rev := v["_rev"].(string)
						doc := &couchdb.Document{ID: id, Rev: rev}
						//                         DB.Delete(doc)
						if _, err := DB.Delete(doc); err != nil {
							msg := fmt.Sprintf("Unable to delete %s %s, %s", rname, status, err)
							logrus.Warn(msg)
						}
					}
				}
			}
		}
	}
}
