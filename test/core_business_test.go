package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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
	dbName := "dummy"
	core.InitCouch("http://127.0.0.1:5984", dbName)
	// create a database
	if _, err = core.Client.Create(dbName); err != nil {
		if strings.Contains(err.Error(), "exists") {
			fmt.Println(err)
		}
		panic(err)
	}
	utils.VERBOSE = 1
	core.Process(record)
	// and finally delete the database
	if _, err = core.Client.Delete(dbName); err != nil {
		panic(err)
	}
}
