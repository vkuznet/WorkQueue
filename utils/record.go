package utils

// Record data-type defines ReqMgr2 data record
type Record map[string]interface{}

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
