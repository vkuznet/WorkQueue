package server

// WorkQueue server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	log "github.com/sirupsen/logrus"

	"github.com/vkuznet/WorkQueue/core"
	"github.com/zemirco/couchdb"

	// web profiler, see https://golang.org/pkg/net/http/pprof
	_ "net/http/pprof"
)

// Config type holds server configuration
type Config struct {
	Workers         int    `json:"Workers"`         // number of workers in WorkQueue
	QueueSize       int    `json:"QueueSize"`       // size of WorkQueue
	MetricsFile     string `json:"MetricsFile"`     // file for metrics output
	MetricsInterval int64  `json:"MetricsInterval"` // interval (in sec) to collect metrics
	RequestType     string `json:"RequestType"`     // ReqMgr2 type of request to fetch
	FetchInterval   int64  `json:"FetchInterval"`   // interval (in sec) to fetch ReqMgr2 data
	CouchUrl        string `json:"CouchURL"`        // couch db url
	Port            int    `json:"port"`            // port number given server runs on, default 8989
	Base            string `json:"base"`            // URL base path for agent server, it will be extracted from Url
	ServerKey       string `json:"serverkey"`       // server key file
	ServerCrt       string `json:"servercrt"`       // server crt file
	LogFormatter    string `json:"LogFormatter"`    // LogFormatter, e.g. json
	LogLevel        string `json:"LogLevel"`        // Log level, e.g. info, warn, err
}

// String returns string representation of Config data type
func (c *Config) String() string {
	return fmt.Sprintf("{Config: Workers=%d QueueSize=%d MetricsFile=%s MetricsInterval=%d RequestType=%s FetchInterval=%d CouchUrl=%s Port=%d Base=%s}", c.Workers, c.QueueSize, c.MetricsFile, c.MetricsInterval, c.RequestType, c.FetchInterval, c.CouchUrl, c.Port, c.Base)
}

// globals used in server/handlers
var _config Config

// Server implementation
func Server(config Config) {
	_config = config

	// open up Catalog DB
	u, err := url.Parse(config.CouchUrl)
	if err != nil {
		panic(err)
	}
	// create a new client
	client, err := couchdb.NewClient(u)
	if err != nil {
		panic(err)
	}

	log.Println("CouchURL", config.CouchUrl)

	// get some information about your CouchDB
	info, err := client.Info()
	if err != nil {
		panic(err)
	}
	log.Println(info)

	db := client.Use("workqueue")
	view := db.View("WorkQueue")
	core.DB = db
	core.VIEW = view

	port := "8989" // default port, the port here is a string type since we'll use it later in http.ListenAndServe
	if config.Port != 0 {
		port = fmt.Sprintf("%d", config.Port)
	}
	base := config.Base
	log.Println("Workqueue", config.String())
	// define handlers
	http.HandleFunc(fmt.Sprintf("%s/", base), AuthHandler)

	// initialize task dispatcher
	dispatcher := core.NewDispatcher(config.Workers, config.QueueSize, config.MetricsFile, config.MetricsInterval)
	dispatcher.Run(config.RequestType, config.FetchInterval)

	if authVar {
		//start HTTPS server which require user certificates
		server := &http.Server{
			Addr: ":" + port,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequestClientCert,
			},
		}
		err = server.ListenAndServeTLS(config.ServerCrt, config.ServerKey)
	} else {
		err = http.ListenAndServe(":"+port, nil) // Start server without user certificates
	}

	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
