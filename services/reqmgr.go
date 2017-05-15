package services

// ReqMgr module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/vkuznet/WorkQueue/utils"
)

// helper function to load ReqMgr data stream
func loadReqMgrData(data []byte) []utils.Record {
	var out []utils.Record
	var rec utils.Record
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
		out = append(out, utils.Convert2Record(r))
	}
	return out
}

// reqMgrUnmarshal unmarshals ReqMgr data stream and return DAS records based on api
func reqMgrUnmarshal(data []byte) []utils.Record {
	return loadReqMgrData(data)
}

// GetRequests function fetches requests from ReqMgr2 data service
func GetRequests(status string) []utils.Record {
	rurl := fmt.Sprintf("%s/request?status=%s", reqmgrUrl(), status)
	resp := utils.FetchResponse(rurl, "")
	data := loadReqMgrData(resp.Data)
	return data
}
