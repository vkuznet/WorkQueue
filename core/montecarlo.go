package core

// WorkQueue MonteCarlo policy implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"github.com/vkuznet/WorkQueue/utils"
	"github.com/zemirco/couchdb"
)

// MonteCarloPolicy defines block policy
type MonteCarloPolicy struct {
	Name   string
	Record utils.Record
	Config utils.Record
}

// Split method satisfy Policy interface
func (b *MonteCarloPolicy) Split() []couchdb.CouchDoc {
	var out []couchdb.CouchDoc
	return out
}

// Split method satisfy Policy interface
func (b *MonteCarloPolicy) Validate() bool {
	return true
}
