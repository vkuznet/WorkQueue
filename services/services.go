package services

// WorkQueue CMS services implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"strings"
	"time"

	"github.com/vkuznet/WorkQueue/utils"
)

func dbsUrl() string {
	return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
}
func phedexUrl() string {
	return "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
}
func sitedbUrl() string {
	return "https://cmsweb.cern.ch/sitedb/data/prod"
}
func reqmgrUrl() string {
	return "https://cmsweb.cern.ch/reqmgr2/data"
}

// Unmarshal
func Unmarshal(r utils.ResponseType) []utils.Record {
	var records []utils.Record
	rurl := strings.ToLower(r.Url)
	switch {
	case strings.Contains(rurl, "phedex"):
		return phedexUnmarshal(r.Data)
	case strings.Contains(rurl, "dbs"):
		return dbsUnmarshal(r.Data)
	case strings.Contains(rurl, "reqmgr"):
		return reqMgrUnmarshal(r.Data)
	case strings.Contains(rurl, "sitedb"):
		return siteDBUnmarshal(r.Data)
	}
	return records
}

// Request represents a request to CMS data service
type Request struct {
	Name string
	Url  string
	Args string
}

// Process concurrently process given set of requests
func Process(requests []Request) []utils.Record {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("Process")

	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for _, req := range requests {
		umap[req.Url] = 1 // keep track of processed urls below
		go utils.Fetch(req.Url, req.Args, out)
	}

	// collect all results from out channel
	var outRecords []utils.Record
	exit := false
	for {
		select {
		case r := <-out:
			record := make(utils.Record)
			var records []utils.Record
			for _, rec := range Unmarshal(r) {
				records = append(records, rec)
			}
			var requestName string
			for _, req := range requests {
				if req.Url == r.Url {
					requestName = req.Name
					break
				}
			}
			record[requestName] = records
			outRecords = append(outRecords, record)
			// remove from umap, indicate that we processed it
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	return outRecords
}
