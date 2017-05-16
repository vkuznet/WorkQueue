package core

// transfer2go core data transfer module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"log" // keep standard log here since we used it in metrics, do not use logrus
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"github.com/vkuznet/WorkQueue/services"
	"github.com/vkuznet/WorkQueue/utils"
)

// Job represents the job to be run with given request
type Job struct {
	Request utils.Record
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

				// perform some work with a job
				Process(job.Request)

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
		requests := services.GetRequests(rtype)
		for _, req := range requests {
			// submit request to processing chain
			go func(req utils.Record) {
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

// Process given request
func Process(record utils.Record) {
	fmt.Println("### Request", record)

	var out []WorkQueueElement
	var inputBlocks, parentData, pileupData map[string][]string
	var numberOfLumis, numberOfFiles, numberOfEvents, jobs, blowupFactor, priority, filesProcessed int
	var parentFlag, openForNewData, noInputUpdate, noPileupUpdate bool
	var mask map[string]int
	var acdc, task, requestName, taskName, dbs, wmSpec, parentQueueUrl, childQueueUrl, wmbsUrl string
	var siteWhiteList, siteBlackList []string
	var percentSuccess, percentComplete float32
	for rname, spec := range record { // reqMgr2 record is {request_name: request_spec}
		switch rec := spec.(type) {
		case map[string]interface{}:
			requestName, _ = rec["RequestName"].(string)
			if rname != requestName {
				logrus.Warn("ReqMgr2 rname=%s != RequestName=%s", rname, requestName)
			}
			taskName = requestName
			dbs, _ = rec["DbsUrl"].(string)
			siteWhiteList, _ = rec["siteWhitelist"].([]string)
			siteBlackList, _ = rec["whiteBlacklist"].([]string)
			priority, _ = rec["RequestPriority"].(int)
			inputDataset, _ := rec["InputDataset"].(string)
			blocks := services.Blocks(inputDataset)
			maskedBlocks := services.MaskedBlocks(blocks)
			var mblocks []string
			for _, mb := range maskedBlocks {
				numberOfLumis += mb.NumberOfLumis()
				numberOfFiles += mb.NumberOfFiles()
				numberOfEvents += mb.NumberOfEvents()
				mblocks = append(mblocks, mb.Block)
			}
			inputBlocks = services.Blocks2Sites(mblocks)
			parentData = services.Blocks2Sites(services.ParentBlocks(mblocks))

			wqe := WorkQueueElement{
				Inputs:          inputBlocks,
				ParentFlag:      parentFlag,
				ParentData:      parentData,
				PileupData:      pileupData,
				NumberOfLumis:   numberOfLumis,
				NumberOfFiles:   numberOfFiles,
				NumberOfEvents:  numberOfEvents,
				Jobs:            jobs,
				OpenForNewData:  openForNewData,
				NoInputUpdate:   noInputUpdate,
				NoPileupUpdate:  noPileupUpdate,
				WMSpec:          wmSpec,
				Mask:            mask,
				BlowupFactor:    blowupFactor,
				ACDC:            acdc,
				Dbs:             dbs,
				TaskName:        taskName,
				Task:            task,
				RequestName:     requestName,
				SiteWhiteList:   siteWhiteList,
				SiteBlackList:   siteBlackList,
				Priority:        priority,
				ParentQueueUrl:  parentQueueUrl,
				ChildQueueUrl:   childQueueUrl,
				PercentSuccess:  percentSuccess,
				PercentComplete: percentComplete,
				WMBSUrl:         wmbsUrl,
				FilesProcessed:  filesProcessed,
			}
			out = append(out, wqe)
		}
	}
	fmt.Println("### WorkQueueElements ###")
	for _, rec := range out {
		fmt.Println(rec)
	}
	// insert WorkQueueElement records into CouchDB
}
