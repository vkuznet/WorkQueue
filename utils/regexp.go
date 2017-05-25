package utils

// WorkQueue/utils - various patterns
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"regexp"
)

var PatternNumber = regexp.MustCompile("^[0-9]")
var PatternUrl = regexp.MustCompile("(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")
