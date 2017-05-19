package core

// WorkQueue  server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rcrowley/go-metrics"
	"github.com/zemirco/couchdb"
)

// Client points to CouchDB client
var Client *couchdb.Client

// DB points to CouchDB
var DB couchdb.DatabaseService

// Metrics of the agent
type Metrics struct {
	Jobs metrics.Counter // number of jobs
}

// WorkqueueStatus data type
type WorkqueueStatus struct {
	Addrs            []string         `json:"addrs"`     // list of all IP addresses
	TimeStamp        int64            `json:"ts"`        // time stamp
	Metrics          map[string]int64 `json:"metrics"`   // workqueue metrics
	NumberOfRequests int              `json:"nRequests"` // number of requests in workqueue
}

// WorkqueueMetrics defines various metrics about the agent work
var WorkqueueMetrics Metrics

// String representation of Metrics
func (m *Metrics) String() string {
	return fmt.Sprintf("<Metrics: jobs=%d>", m.Jobs.Count())
}

// ToDict converts Metrics structure to a map
func (m *Metrics) ToDict() map[string]int64 {
	dict := make(map[string]int64)
	if m.Jobs != nil {
		dict["jobs"] = m.Jobs.Count()
	}
	return dict
}

// InitCouch initializes connection to CouchDB
func InitCouch(couchUrl, dbName string) {
	// open up Catalog DB
	u, err := url.Parse(couchUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"couchUrl": couchUrl,
			"step":     "urlParse",
		}).Panic(err)
	}

	// create a new client
	client, err := couchdb.NewClient(u)
	if err != nil {
		log.WithFields(log.Fields{
			"couchUrl": couchUrl,
			"url":      u,
			"step":     "couchdb.NewClient",
		}).Panic(err)
	}
	Client = client

	// get some information about your CouchDB
	info, err := client.Info()
	if err != nil {
		log.WithFields(log.Fields{
			"couchUrl": couchUrl,
			"url":      u,
			"client":   client,
			"step":     "client.Info()",
		}).Panic(err)
	}
	log.Println(info)

	// create a database if it does not exists
	if _, err = client.Create(dbName); err != nil {
		if strings.Contains(err.Error(), "exists") {
			log.Warn(err)
		} else {
			log.WithFields(log.Fields{
				"couchUrl": couchUrl,
				"url":      u,
				"client":   client,
				"dbname":   dbName,
				"step":     "client.Create(dbName)",
			}).Panic(err)
		}
	}

	DB = client.Use(dbName)
}
