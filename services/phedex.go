package services

// Phedex module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

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
		fmt.Println(msg)
	}
	out = append(out, rec)
	return out
}

// PhedexUnmarshal unmarshals Phedex data stream and return DAS records based on api
func phedexUnmarshal(data []byte) []utils.Record {
	return loadPhedexData(data)
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
	var out []string
	for _, rec := range phedexUnmarshal(resp.Data) {
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
