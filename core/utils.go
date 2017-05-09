package core

// workqueue agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rcrowley/go-metrics"
	"github.com/vkuznet/workqueue/utils"
	"github.com/zemirco/couchdb"
)

var DB couchdb.DatabaseService
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

type Record map[string]interface{}

// Convert2Record converts given interface to Record data type
func Convert2Record(item interface{}) Record {
	switch r := item.(type) {
	case map[string]interface{}:
		rec := make(Record)
		for kkk, vvv := range r {
			rec[kkk] = vvv
		}
		return rec
	case Record:
		return r
	}
	return nil
}

func loadReqMgr2Data(data []byte) []Record {
	var out []Record
	var rec Record
	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&rec)

	// original way to decode data
	// err := json.Unmarshal(data, &rec)

	if err != nil {
		msg := fmt.Sprintf("ReqMgr unable to unmarshal data, data=%s, error=%v", string(data), err)
		log.Println(msg)
	}
	for _, r := range rec["result"].([]interface{}) {
		out = append(out, Convert2Record(r))
	}
	return out

}
func GetRequests(status string) []Record {
	rurl := fmt.Sprintf("https://cmsweb.cern.ch/reqmgr2/data/request?status=%s", status)
	resp := utils.FetchResponse(rurl, "")
	data := loadReqMgr2Data(resp.Data)
	/*
		for _, rec := range data {
			for key, val := range rec {
				r := Convert2Record(val)
				fmt.Println(key, r["Teams"], r["RequestWorkflow"])
			}
		}
	*/
	return data
}

// helper function to parse ReqMgr2 record
// the code may likely to change, since I don't know yet which attributes of
// ReqMgr2 record will be necessary for WorkQueue
func parseRequest(record Record) []Record {
	var out []Record
	for _, val := range record {
		switch rec := val.(type) {
		case map[string]interface{}:
			name, _ := rec["RequestName"].(string)
			status, _ := rec["RequestStatus"].(string)
			rtype, _ := rec["RequestType"].(string)
			workflow, _ := rec["RequestWorkflow"].(string)
			teams, _ := rec["Teams"]
			r := make(Record)
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
