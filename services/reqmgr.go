package services

// ReqMgr module
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

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
	rurl := fmt.Sprintf("%s/data/request?status=%s", reqmgrUrl(), status)
	resp := utils.FetchResponse(rurl, "")
	data := loadReqMgrData(resp.Data)
	return data
}

// GetRequest function fetches request from ReqMgr2 for given request name
func GetRequest(name string) []utils.Record {
	rurl := fmt.Sprintf("%s/data/request?name=%s", reqmgrUrl(), name)
	resp := utils.FetchResponse(rurl, "")
	data := loadReqMgrData(resp.Data)
	return data
}

// RequestConfig fetch reqmgr record configuration
func RequestConfig(name string) utils.Record {
	rurl := fmt.Sprintf("%s/config?name=%s", reqmgrUrl(), name)
	resp := utils.FetchResponse(rurl, "")
	reqConfig := make(utils.Record)
	pat := fmt.Sprintf("%s.", name)
	if resp.Error == nil {
		data := string(resp.Data)
		for _, line := range strings.Split(data, "<br/>") {
			if strings.Contains(line, "=") {
				arr := strings.Split(strings.Replace(line, pat, "", 1), "=")
				if len(arr) == 2 {
					val := strings.Trim(arr[1], " ")
					key := strings.Trim(arr[0], " ")
					if val == "[]" {
						reqConfig[key] = []string{}
					} else if val == "False" {
						reqConfig[key] = false
					} else if val == "True" {
						reqConfig[key] = true
					} else if utils.PatternNumber.MatchString(val) {
						v, e := strconv.ParseInt(val, 10, 64)
						if e == nil {
							reqConfig[key] = v
						} else {
							reqConfig[key] = val
						}
					} else {
						v := strings.Replace(val, "'", "", -1)
						if strings.Contains(v, ",") || strings.Contains(v, "[") {
							reqConfig[key] = stringArray(v, ",")
						} else {
							reqConfig[arr[0]] = v
						}
					}
				}
			}
		}
	}
	return reqConfig
}

// helper function to return array of strings from given string and separator
func stringArray(val, sep string) []string {
	v := strings.Replace(val, "[", "", -1)
	v = strings.Replace(v, "]", "", -1)
	var out []string
	for _, v := range strings.Split(v, sep) {
		out = append(out, strings.Trim(v, " "))
	}
	return out
}
