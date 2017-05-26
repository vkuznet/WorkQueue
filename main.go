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
	"runtime"
	"strings"
	"time"

	"github.com/pkg/profile"
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
	mode := flag.String("profileMode", "", "enable profiling mode, one of [cpu, mem, block]")
	flag.Parse()
	switch *mode {
	case "cpu":
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
	default:
		// do nothing
	}

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
	// measure in backround memory usage of the server
	var m runtime.MemStats
	go func() {
		for {
			if utils.VERBOSE > 0 {
				runtime.ReadMemStats(&m)
				log.WithFields(log.Fields{
					"system":  m.HeapSys,
					"alloc":   m.HeapAlloc,
					"idle":    m.HeapIdle,
					"release": m.HeapReleased,
				}).Info("heap memory")
			}
			time.Sleep(time.Duration(1) * time.Second) // wait for a job
		}
	}()
	// Start the server
	server.Server(config)
}
