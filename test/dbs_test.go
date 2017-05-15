package main

import (
	"fmt"
	"testing"

	"github.com/vkuznet/WorkQueue/services"
)

// TestRunLumis tests services.RunLumis behavior
func TestRunLumis(t *testing.T) {
	expect := fmt.Sprintf("{Run: 123, Lumis: [1 2 3]}")
	runLumis := services.RunLumis{Run: 123, Lumis: []int64{1, 2, 3}}
	if runLumis.String() != expect {
		t.Errorf("wrong string representation of runLumis, %s != %s", runLumis, expect)
	}
}

// TestMaskedBlock tests services.MaskedBlock behavior
func TestMaskedBlock(t *testing.T) {
	var filesLumis []services.FileLumis
	runLumis := services.RunLumis{Run: 123, Lumis: []int64{1, 2, 3}}
	fileLumis := services.FileLumis{Lfn: "a.root", RunLumis: runLumis}
	filesLumis = append(filesLumis, fileLumis)
	filesLumis = append(filesLumis, fileLumis)
	maskedBlock := services.MaskedBlock{Block: "/a/b/c#123", FilesLumis: filesLumis}
	nfiles := maskedBlock.NumberOfFiles()
	expect := 2
	if nfiles != expect {
		t.Errorf("wrong number of files: nfiles=%d != expect=%d", nfiles, expect)
	}
	nlumis := maskedBlock.NumberOfLumis()
	expect = 3 * 2 // we have two files where each file has 3 lumis
	if nlumis != expect {
		t.Errorf("wrong number of lumis: nlumis=%d != expect=%d", nfiles, expect)
	}
}

// TestDataset is an integration test to fetch masked blocks from given dataset
func TestDataset(t *testing.T) {
	dataset := "/RelValPhotonJets_Pt_10_13/CMSSW_9_1_0_pre3-91X_upgrade2017_realistic_v3-v1/DQMIO"
	blocks := services.Blocks(dataset)
	mBlocks := services.MaskedBlocks(blocks)
	fmt.Println(dataset)
	for _, r := range mBlocks {
		fmt.Println("MaskedBlock", r)
		fmt.Println("Number of files", r.NumberOfFiles())
		fmt.Println("Number of lumis", r.NumberOfLumis())
	}
}

// TestSites is an integration test to fetch sites for given dataset or block
func TestSites(t *testing.T) {
	dataset := "/WJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer15GS-MCRUN2_71_V1_ext2-v1/GEN-SIM"
	sites := services.Sites(dataset)
	fmt.Println(sites)
	blocks := services.Blocks(dataset)
	for idx, block := range blocks {
		sites := services.Sites(block)
		fmt.Println(block, sites)
		if idx == 2 {
			break
		}
	}
}
