package core

// WorkQueue Queue implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

// WorkQueueElement structure
type WorkQueueElement struct {
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
	Task            string
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
}

// BlockPolicy defines block policy
type BlockPolicy struct {
	Name              string
	WorkQueueElements []WorkQueueElement
}

// DatasetPolicy defines dataset policy
type DatasetPolicy struct {
	Name              string
	WorkQueueElements []WorkQueueElement
}

// MonteCarloPolicy defines MC policy
type MonteCarloPolicy struct {
	Name              string
	WorkQueueElements []WorkQueueElement
}
