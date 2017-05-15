package services

// WorkQueue CMS services implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"strings"
	"time"

	"github.com/vkuznet/WorkQueue/utils"
)

// Unmarshal
func Unmarshal(r utils.ResponseType) []Record {
	var records []Record
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
func Process(requests []Request) []Record {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("Process")

	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for _, req := range requests {
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(req.Url, req.Args, out)
	}

	// collect all results from out channel
	var outRecords []Record
	exit := false
	for {
		select {
		case r := <-out:
			record := make(Record)
			var records []Record
			for _, rec := range Unmarshal(r) {
				records = append(records, rec)
			}
			record[r.Name] = records
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
