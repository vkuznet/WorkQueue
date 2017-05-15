package core

// WorkQueue Block policy implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"github.com/vkuznet/WorkQueue/utils"
)

// BlockPolicy defines block policy
type BlockPolicy struct {
	Name string
	Spec utils.Record
}

// Split method satisfy Policy interface
func (b *BlockPolicy) Split() []WorkQueueElement {
	var out []WorkQueueElement
	return out
}

// Split method satisfy Policy interface
func (b *BlockPolicy) Validate() bool {
	return true
}
