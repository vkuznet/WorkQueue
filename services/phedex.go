package services

// Phedex module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/WorkQueue/utils"
)

// helper function to load data stream and return DAS records
func loadPhedexData(data []byte) []utils.Record {
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
		msg := fmt.Sprintf("Phedex unable to unmarshal the data, data=%s, error=%v", string(data), err)
		log.Error(msg)
		return out
	}
	out = append(out, rec)
	return out
}

// PhedexUnmarshal unmarshals Phedex data stream and return DAS records based on api
func phedexUnmarshal(data []byte) []utils.Record {
	return loadPhedexData(data)
}

// helper function to extract phedex node from phedex response record
func phedexNode(data []byte) []string {
	var out []string
	for _, rec := range phedexUnmarshal(data) {
		if rec["phedex"] != nil {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				replicas := brec["replica"].([]interface{})
				for _, val := range replicas {
					row := val.(map[string]interface{})
					node := ""
					if row["node"] != nil {
						node = row["node"].(string)
						out = append(out, node)
					}
				}
			}
		}
	}
	return utils.List2Set(out)
}

// Sites look-ups site names for given input (dataset or block)
func Sites(input string) []string {
	rurl := fmt.Sprintf("%s/blockReplicas?dataset=%s", phedexUrl(), url.PathEscape(input))
	if strings.Contains(input, "#") {
		rurl = fmt.Sprintf("%s/blockReplicas?block=%s", phedexUrl(), url.PathEscape(input))
	}
	resp := utils.FetchResponse(rurl, "")
	if resp.Error != nil {
		fmt.Println(resp.Error)
	}
	return phedexNode(resp.Data)
}

// Blocks2Sites fetches site lists for given list of blocks
func Blocks2Sites(blocks []string) map[string][]string {
	var requests []Request
	for _, block := range blocks {
		rurl := fmt.Sprintf("%s/blockReplicas?block=%s", phedexUrl(), url.PathEscape(block))
		req := Request{Name: block, Url: rurl, Args: ""}
		requests = append(requests, req)
	}
	bSites := make(map[string][]string)
	for _, rec := range Process(requests) { // key here is index, rec = {ReqName: []Records}
		for block, row := range rec { // key here is block (Request.Name)
			switch r := row.(type) {
			case []utils.Record:
				var sites []string
				for _, phedexRecord := range r {
					data, _ := json.Marshal(phedexRecord)
					for _, node := range phedexNode([]byte(data)) {
						sites = append(sites, node)
					}
				}
				bSites[block] = sites
			}
		}
	}
	return bSites
}
