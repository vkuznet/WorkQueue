package services

// Phedex module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"

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
	var out []utils.Record
	records := loadPhedexData(data)
	for _, rec := range records {
		out = append(out, rec)
	}
	return out
}
