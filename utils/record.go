package utils

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

// Record data-type defines ReqMgr2 data record
type Record map[string]interface{}

// String function implements Stringer interface
func (r Record) String() string {
	rec, err := json.Marshal(r)
	if err != nil {
		log.Fatal(err)
	}
	return string(rec)
}

// Convert2Record converts given interface to Record data type
func Convert2Record(item interface{}) Record {
	switch r := item.(type) {
	case map[string]interface{}:
		rec := make(Record)
		for kkk, vvv := range r {
			rec[kkk] = vvv
		}
		return rec
	case Record:
		return r
	}
	return nil
}
