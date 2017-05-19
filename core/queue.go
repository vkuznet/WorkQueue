package core

import (
	"encoding/json"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// WorkQueue Queue implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

// WorkQueueElement structure
type WorkQueueElement struct {
	couchdb.Document
	Inputs          map[string][]string // {blockName:sites}
	ParentData      map[string][]string // {blockName:sites}
	PileupData      map[string][]string // {blockName:sites}
	ParentFlag      bool
	NumberOfLumis   int
	NumberOfFiles   int
	NumberOfEvents  int
	Jobs            int
	OpenForNewData  bool
	NoInputUpdate   bool
	NoPileupUpdate  bool
	WMSpec          string
	Mask            map[string]int
	BlowupFactor    int
	ACDC            string
	RequestName     string
	TaskName        string
	Dbs             string
	SiteWhiteList   []string
	SiteBlackList   []string
	Priority        int
	ParentQueueUrl  string
	ChildQueueUrl   string
	PercentSuccess  float32
	PercentComplete float32
	WMBSUrl         string
	FilesProcessed  int
	StartPolicy     string
	EndPolicy       string
}

// Mask data structure keeps track of run-lumi
type Mask struct {
	InclusiveMask bool
	FirstEvent    int64
	LastEvent     int64
	FirstLumi     int
	LastLumi      int
	FirstRun      int
	LstRun        int
	RunAndLumis   map[int][]int
}

// Policy interface defines policy methods
type Policy interface {
	Split() []WorkQueueElement
	Validate() bool
}

// String function implements Stringer interface
func (w WorkQueueElement) String() string {
	rec, err := json.Marshal(w)
	if err != nil {
		log.Fatal(err)
	}
	return string(rec)
}

// helper function to find value in reqmgr config record
func configValue(config utils.Record, key string) interface{} {
	for k, v := range config {
		if strings.Contains(k, key) {
			return v
		}
	}
	return nil
}
