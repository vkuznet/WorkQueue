package core

// WorkQueue Block policy implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/WorkQueue/services"
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// BlockPolicy defines block policy
type BlockPolicy struct {
	Name   string
	Record utils.Record
	Config utils.Record
}

// Split method satisfy Policy interface
func (b *BlockPolicy) Split() []couchdb.CouchDoc {
	var out []couchdb.CouchDoc
	var inputBlocks, parentData, pileupData map[string][]string
	var numberOfLumis, numberOfFiles, numberOfEvents, jobs, blowupFactor, priority, filesProcessed int
	var parentFlag, openForNewData, noInputUpdate, noPileupUpdate bool
	var mask map[string]int
	var acdc, requestName, taskName, dbs, wmSpec, parentQueueUrl, childQueueUrl, wmbsUrl string
	var siteWhiteList, siteBlackList []string
	var percentSuccess, percentComplete float32
	for rname, spec := range b.Record { // reqMgr2 record is {request_name: request_spec}
		switch rec := spec.(type) {
		case map[string]interface{}:
			requestName, _ = rec["RequestName"].(string)
			if rname != requestName {
				log.Warn("ReqMgr2 rname=%s != RequestName=%s", rname, requestName)
			}
			taskName = requestName
			dbs, _ = rec["DbsUrl"].(string)
			siteWhiteList, _ = rec["siteWhitelist"].([]string)
			siteBlackList, _ = rec["whiteBlacklist"].([]string)
			priority, _ = rec["RequestPriority"].(int)
			wmSpec, _ = rec["RequestWorkflow"].(string)
			inputDataset, _ := rec["InputDataset"].(string)
			blocks := services.Blocks(inputDataset)
			maskedBlocks := services.MaskedBlocks(blocks)
			var mblocks []string
			for _, mb := range maskedBlocks {
				numberOfLumis += mb.NumberOfLumis()
				numberOfFiles += mb.NumberOfFiles()
				numberOfEvents += mb.NumberOfEvents()
				mblocks = append(mblocks, mb.Block)
			}
			inputBlocks = services.Blocks2Sites(mblocks)
			parentData = services.Blocks2Sites(services.ParentBlocks(mblocks))

			wqe := &WorkQueueElement{
				Inputs:          inputBlocks,
				ParentFlag:      parentFlag,
				ParentData:      parentData,
				PileupData:      pileupData,
				NumberOfLumis:   numberOfLumis,
				NumberOfFiles:   numberOfFiles,
				NumberOfEvents:  numberOfEvents,
				Jobs:            jobs,
				OpenForNewData:  openForNewData,
				NoInputUpdate:   noInputUpdate,
				NoPileupUpdate:  noPileupUpdate,
				WMSpec:          wmSpec,
				Mask:            mask,
				BlowupFactor:    blowupFactor,
				ACDC:            acdc,
				Dbs:             dbs,
				TaskName:        taskName,
				RequestName:     requestName,
				SiteWhiteList:   siteWhiteList,
				SiteBlackList:   siteBlackList,
				Priority:        priority,
				ParentQueueUrl:  parentQueueUrl,
				ChildQueueUrl:   childQueueUrl,
				PercentSuccess:  percentSuccess,
				PercentComplete: percentComplete,
				WMBSUrl:         wmbsUrl,
				FilesProcessed:  filesProcessed,
			}
			out = append(out, wqe)
		}
	}
	return out
}

// Split method satisfy Policy interface
func (b *BlockPolicy) Validate() bool {
	return true
}
