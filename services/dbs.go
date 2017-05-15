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

// helper function to retrieve list of blocks for a given dataset
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
