package services

// DBS module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	log "github.com/sirupsen/logrus"
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
		log.Error(msg)
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

// RunLumis keep track of run-lumis
type RunLumis struct {
	Run    int64
	Lumis  []int64
	Events []int64
}

// String implements Stringer interface
func (r RunLumis) String() string {
	return fmt.Sprintf("{Run: %d, Lumis: %v}", r.Run, r.Lumis)
}

// NumberOfLumis returns number of lumis in RunLumis
func (r RunLumis) NumberOfLumis() int {
	return len(r.Lumis)
}

// NumberOfEvents returns number of lumis in RunLumis
func (r RunLumis) NumberOfEvents() int {
	return len(r.Events)
}

// FileLumis keep track of block content
type FileLumis struct {
	Lfn      string
	RunLumis RunLumis
}

// String implements Stringer interface
func (r FileLumis) String() string {
	return fmt.Sprintf("{Lfn: %s, RunLumis: %v}", r.Lfn, r.RunLumis)
}

// NumberOfLumis returns number of lumis in FileLumis
func (r FileLumis) NumberOfLumis() int {
	return r.RunLumis.NumberOfLumis()
}

// NumberOfEvents returns number of lumis in FileEvents
func (r FileLumis) NumberOfEvents() int {
	return r.RunLumis.NumberOfEvents()
}

// MaskedBlock represents block record with list of fileLumis
type MaskedBlock struct {
	Block      string
	FilesLumis []FileLumis
}

// String implements Stringer interface
func (r MaskedBlock) String() string {
	return fmt.Sprintf("{Block: %s, FilesLumis: %v}", r.Block, r.FilesLumis)
}

// NumberOfFiles returns number of files in MaskedBlock
func (r MaskedBlock) NumberOfFiles() int {
	return len(r.FilesLumis)
}

// NumberOfLumis returns number of lumis in MaskedBlock
func (r MaskedBlock) NumberOfLumis() int {
	tot := 0
	for _, fileLumis := range r.FilesLumis {
		tot += fileLumis.NumberOfLumis()
	}
	return tot
}

// NumberOfEvents returns number of lumis in MaskedBlock
func (r MaskedBlock) NumberOfEvents() int {
	tot := 0
	for _, fileLumis := range r.FilesLumis {
		tot += fileLumis.NumberOfEvents()
	}
	return tot
}

// helper function to fetch event info for blocks
func blockEvents(blocks []string) map[string]int64 {
	var requests []Request
	for _, block := range blocks {
		rurl := fmt.Sprintf("%s/filesummaries?block_name=%s", dbsUrl(), url.PathEscape(block))
		req := Request{Name: block, Url: rurl, Args: ""}
		requests = append(requests, req)
	}
	blockInfo := make(map[string]int64)
	for _, rec := range Process(requests) { // key here is index, rec = {ReqName: []Records}
		for block, row := range rec { // key here is block request.Name
			switch r := row.(type) {
			case []utils.Record:
				for _, vvv := range r {
					evts, _ := vvv["num_event"].(json.Number).Int64()
					blockInfo[block] = evts
				}
			}
		}
	}
	return blockInfo
}

// MaskedBlocks returns record of block details mased by provided lumis
func MaskedBlocks(blocks []string) []MaskedBlock {
	var requests []Request
	for _, block := range blocks {
		rurl := fmt.Sprintf("%s/filelumis?block_name=%s", dbsUrl(), url.PathEscape(block))
		req := Request{Name: block, Url: rurl, Args: ""}
		requests = append(requests, req)
	}
	var out []MaskedBlock
	var blockInfo map[string]int64
	for _, rec := range Process(requests) { // key here is index, rec = {ReqName: []Records}
		for block, row := range rec { // key here is block request.Name
			switch r := row.(type) {
			case []utils.Record:
				var filesLumis []FileLumis
				for _, vvv := range r {
					lfn := vvv["logical_file_name"].(string)
					run, _ := vvv["run_num"].(json.Number).Int64()
					var lumis []int64
					for _, v := range vvv["lumi_section_num"].([]interface{}) {
						lumiNumber, _ := v.(json.Number).Int64()
						lumis = append(lumis, lumiNumber)
					}
					var events []int64
					if _, ok := vvv["event_count"]; ok {
						for _, v := range vvv["event_count"].([]interface{}) {
							event, _ := v.(json.Number).Int64()
							events = append(events, event)
						}
					} else { // back-up solution
						if blockInfo == nil { // first fatch all info about blocks
							blockInfo = blockEvents(blocks)
						}
						if evts, ok := blockInfo[block]; ok {
							events = append(events, evts)
						}
					}
					runLumis := RunLumis{Run: run, Lumis: lumis, Events: events}
					fileLumis := FileLumis{Lfn: lfn, RunLumis: runLumis}
					filesLumis = append(filesLumis, fileLumis)
				}
				maskedBlock := MaskedBlock{Block: block, FilesLumis: filesLumis}
				out = append(out, maskedBlock)
			}
		}
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
