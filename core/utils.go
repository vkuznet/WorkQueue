package core

// WorkQueue  server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// DB points to CouchDB
var DB couchdb.DatabaseService

// VIEW represents CouchDB view
var VIEW couchdb.ViewService

// Metrics of the agent
type Metrics struct {
	Jobs metrics.Counter // number of jobs
}

// WorkqueueStatus data type
type WorkqueueStatus struct {
	Addrs            []string         `json:"addrs"`     // list of all IP addresses
	TimeStamp        int64            `json:"ts"`        // time stamp
	Metrics          map[string]int64 `json:"metrics"`   // workqueue metrics
	NumberOfRequests int              `json:"nRequests"` // number of requests in workqueue
}

// WorkqueueMetrics defines various metrics about the agent work
var WorkqueueMetrics Metrics

// String representation of Metrics
func (m *Metrics) String() string {
	return fmt.Sprintf("<Metrics: jobs=%d>", m.Jobs.Count())
}

// ToDict converts Metrics structure to a map
func (m *Metrics) ToDict() map[string]int64 {
	dict := make(map[string]int64)
	if m.Jobs != nil {
		dict["jobs"] = m.Jobs.Count()
	}
	return dict
}

// helper function to parse ReqMgr2 record
// the code may likely to change, since I don't know yet which attributes of
// ReqMgr2 record will be necessary for WorkQueue
func parseRequest(record utils.Record) []utils.Record {
	var out []utils.Record
	for _, val := range record {
		switch rec := val.(type) {
		case map[string]interface{}:
			name, _ := rec["RequestName"].(string)
			status, _ := rec["RequestStatus"].(string)
			rtype, _ := rec["RequestType"].(string)
			workflow, _ := rec["RequestWorkflow"].(string)
			teams, _ := rec["Teams"]
			r := make(utils.Record)
			r["name"] = name
			r["status"] = status
			r["type"] = rtype
			r["workflow"] = workflow
			r["teams"] = teams
			out = append(out, r)
		}
	}
	return out
}

// helper function to parse ReqMgr2 record
// the code may likely to change, since I don't know yet which attributes of
// ReqMgr2 record will be necessary for WorkQueue
func request2WQE(record utils.Record) []WorkQueueElement {
	var out []WorkQueueElement
	var inputBlocks, parentData, pileupData map[string][]string
	var numberOfLumis, numberOfFiles, numberOfEvents, jobs, blowupFactor, priority, filesProcessed int
	var parentFlag, openForNewData, noInputUpdate, noPileupUpdate bool
	var mask map[string]int
	var acdc, task, requestName, taskName, dbs, wmSpec, parentQueueUrl, childQueueUrl, wmbsUrl string
	var siteWhiteList, siteBlackList []string
	var percentSuccess, percentComplete float32
	for _, val := range record {
		switch rec := val.(type) {
		case map[string]interface{}:
			requestName, _ = rec["RequestName"].(string)
			taskName = requestName
			dbs, _ = rec["DbsUrl"].(string)
			siteWhiteList, _ = rec["siteWhitelist"].([]string)
			siteBlackList, _ = rec["whiteBlacklist"].([]string)
			priority, _ = rec["InitialPriority"].(int)
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
	return out
}
