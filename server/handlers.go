package server

// WorkQueue server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/segmentio/pointer"
	"github.com/vkuznet/WorkQueue/core"
	"github.com/vkuznet/WorkQueue/services"
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// global variable which we initialize once
var _userDNs []string
var authVar bool

func userDNs() []string {
	var out []string
	rurl := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	resp := utils.FetchResponse(rurl, "")
	if resp.Error != nil {
		log.Println("ERROR unable to fetch SiteDB records", resp.Error)
		return out
	}
	var rec map[string]interface{}
	err := json.Unmarshal(resp.Data, &rec)
	if err != nil {
		log.Println("ERROR unable to unmarshal response", err)
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	var idx int
	for i, h := range headers {
		if h.(string) == "dn" {
			idx = i
			break
		}
	}
	values := rec["result"].([]interface{})
	for _, item := range values {
		val := item.([]interface{})
		v := val[idx]
		if v != nil {
			out = append(out, v.(string))
		}
	}
	return out
}

// func init() {
//	_userDNs = userDNs()
//}

// Init is custom initialization function, we don't use init() because we want
// control of authentication from command line
func Init(authArg bool) {
	authVar = authArg
	if authVar {
		_userDNs = userDNs()
	}
}

// custom logic for CMS authentication, users may implement their own logic here
func auth(r *http.Request) bool {

	if !authVar {
		return true
	}

	if utils.VERBOSE > 1 {
		dump, err := httputil.DumpRequest(r, true)
		log.Println("AuthHandler HTTP request", r, string(dump), err)
	}
	userDN := utils.UserDN(r)
	match := utils.InList(userDN, _userDNs)
	if !match {
		log.Println("ERROR Auth userDN", userDN, "not found in SiteDB")
	}
	return match
}

// AuthHandler authenticate incoming requests and route them to appropriate handler
func AuthHandler(w http.ResponseWriter, r *http.Request) {
	// check if server started with hkey file (auth is required)
	status := auth(r)
	if !status {
		msg := "You are not allowed to access this resource"
		http.Error(w, msg, http.StatusForbidden)
		return
	}
	arr := strings.Split(r.URL.Path, "/")
	path := arr[len(arr)-1]
	switch path {
	case "status":
		StatusHandler(w, r)
	case "requests":
		RequestHandler(w, r)
	default:
		DefaultHandler(w, r)
	}
}

// GET methods

// StatusHandler provides information about the agent
func StatusHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// find our how many request in workqueue
	params := couchdb.QueryParameters{
		Group: pointer.Bool(true),
	}
	view := core.DB.View("WorkQueue")
	res, err := view.Get("elementsByWorkflow", params)
	if err != nil {
		panic(err)
	}
	addrs := utils.HostIP()
	astats := core.WorkqueueStatus{Addrs: addrs, TimeStamp: time.Now().Unix(), Metrics: core.WorkqueueMetrics.ToDict(), NumberOfRequests: len(res.Rows)}
	data, err := json.Marshal(astats)
	if err != nil {
		log.Println("ERROR StatusHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// RequestHandler provides information about the agent
func RequestHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// get requests for given type
	rtype := r.FormValue("type")
	requests := services.GetRequests(rtype)
	data, err := json.Marshal(requests)
	if err != nil {
		log.Println("ERROR StatusHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// DefaultHandler provides information about the agent
func DefaultHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	// TODO: implement here default page for data-service
	// should be done via templates
	msg := fmt.Sprintf("Default page: %v\n", time.Now())
	w.Write([]byte(msg))
}

// POST methods
