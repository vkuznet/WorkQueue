package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/vkuznet/WorkQueue/core"
	"github.com/vkuznet/WorkQueue/utils"
)

// TestCoreProcess tests core.Process behavior
func TestCoreProcess(t *testing.T) {
	cdir, err := os.Getwd()
	fname := fmt.Sprintf("%s/data/assigned.json", cdir)
	c, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Unable to read %s", fname)
	}
	var record utils.Record
	err = json.Unmarshal(c, &record)
	if err != nil {
		t.Errorf("Unable to unmarshall data %v", string(c))
	}
	core.Process(record)
}
