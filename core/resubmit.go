package core

// WorkQueue ResubmitBlock policy implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// ResubmitBlockPolicy defines block policy
type ResubmitBlockPolicy struct {
	Name   string
	Record utils.Record
}

// Split method satisfy Policy interface
func (b *ResubmitBlockPolicy) Split() []couchdb.CouchDoc {
	var out []couchdb.CouchDoc
	return out
}

// Split method satisfy Policy interface
func (b *ResubmitBlockPolicy) Validate() bool {
	return true
}
