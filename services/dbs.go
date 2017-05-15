package services

// DBS module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/vkuznet/WorkQueue/utils"
)

// helper function to load DBS data stream
func loadDBSData(data []byte) []utils.Record {
	var out []utils.Record

	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&out)

	// original way to decode data
	// err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("DBS unable to unmarshal the data, data=%s, error=%v", string(data), err)
		fmt.Println(msg)
	}
	return out
}

// dbsUnmarshal unmarshals DBS data stream records
func dbsUnmarshal(data []byte) []utils.Record {
	records := loadDBSData(data)
	var out []utils.Record
	for _, rec := range records {
		out = append(out, rec)
	}
	return out
}

// Blocks function retrieves list of blocks for a given dataset
func Blocks(dataset string) []string {
	rurl := fmt.Sprintf("%s/blocks?dataset=%s", dbsUrl(), url.PathEscape(dataset))
	resp := utils.FetchResponse(rurl, "")
	if resp.Error != nil {
		fmt.Println(resp.Error)
	}
	var out []string
	for _, rec := range dbsUnmarshal(resp.Data) {
		blk := rec["block_name"].(string)
		out = append(out, blk)
	}
	return out
}

// ParentBlocks function retrieves block parents for given list of blocks
func ParentBlocks(blocks []string) []string {
	var requests []Request
	// TODO: DBS provides blockparents GET API and blockparents POST API
	// it is unclear which API to use here
	for _, block := range blocks {
		rurl := fmt.Sprintf("%s/blockparents?block_name=%s", dbsUrl(), url.PathEscape(block))
		req := Request{Name: block, Url: rurl, Args: ""}
		requests = append(requests, req)
	}
	var out []string
	for _, rec := range Process(requests) { // key here is index, rec = {ReqName: []Records}
		for _, row := range rec { // key here is block request.Name
			switch r := row.(type) {
			case []utils.Record:
				for _, vvv := range r {
					switch val := vvv["parent_block_name"].(type) {
					case string:
						out = append(out, val)
					}
				}
			}
		}
	}
	return out
}
