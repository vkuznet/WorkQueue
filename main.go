// WorkQueue - Go implementation of DMWM WorkQueue
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/vkuznet/WorkQueue/server"
	"github.com/vkuznet/WorkQueue/utils"
)

func main() {

	// server options
	var configFile string
	flag.StringVar(&configFile, "config", "", "configuration file")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "verbose level")
	var authVar bool
	flag.BoolVar(&authVar, "auth", true, "To disable the auth layer")
	flag.Parse()

	if authVar {
		utils.CheckX509()
	}
	utils.VERBOSE = verbose
	if configFile == "" {
		log.Println("Please provide configuration file")
		os.Exit(1)
	}
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("Unable to read", configFile, err)
		os.Exit(1)
	}
	var config server.Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Println("Unable to parse", configFile, err)
		os.Exit(1)
	}
	// set logger settings depending on configuration
	if strings.Contains(strings.ToLower(config.LogFormatter), "json") {
		log.SetFormatter(&log.JSONFormatter{})
	}
	if strings.Contains(strings.ToLower(config.LogLevel), "info") {
		log.SetLevel(log.InfoLevel)
	}
	if strings.Contains(strings.ToLower(config.LogLevel), "warn") {
		log.SetLevel(log.WarnLevel)
	}
	if strings.Contains(strings.ToLower(config.LogLevel), "err") {
		log.SetLevel(log.ErrorLevel)
	}
	if strings.Contains(strings.ToLower(config.LogLevel), "debug") {
		utils.VERBOSE = 2 // overwrite default since we want to get deep view
		log.SetLevel(log.DebugLevel)
	}
	log.Println("VERBOSE", utils.VERBOSE)
	server.Server(config)
}
