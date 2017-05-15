package services

// SiteDB module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vkuznet/WorkQueue/utils"
)

// helper function to load SiteDB data stream
func loadSiteDBData(data []byte) []utils.Record {
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
		msg := fmt.Sprintf("SiteDB unable to unmarshal the data, data=%s, error=%v", string(data), err)
		fmt.Println(msg)
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	values := rec["result"].([]interface{})
	for _, item := range values {
		row := make(utils.Record)
		val := item.([]interface{})
		for i, h := range headers {
			key := h.(string)
			row[key] = val[i]
			if key == "username" {
				row["name"] = row[key]
			}
		}
		out = append(out, row)
	}
	return out
}

// siteDBUnmarshal unmarshals SiteDB data stream and return DAS records based on api
func siteDBUnmarshal(data []byte) []utils.Record {
	records := loadSiteDBData(api, data)
	return records
}
