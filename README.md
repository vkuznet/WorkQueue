# WorkQueue
Go-implementation of CMS DMWM WorkQueue

[![Build Status](https://travis-ci.org/vkuznet/WorkQueue.svg?branch=master)](https://travis-ci.org/vkuznet/WorkQueue)
[![Go Report Card](https://goreportcard.com/badge/github.com/vkuznet/WorkQueue)](https://goreportcard.com/report/github.com/vkuznet/WorkQueue)
[![GoDoc](https://godoc.org/github.com/vkuznet/WorkQueue?status.svg)](https://godoc.org/github.com/vkuznet/WorkQueue)

### WorkQueue
[WorkQueue](https://github.com/dmwm/WMCore/wiki/WorkQueue) is CMS DMWM component to schedule WMAgent jobs

### Build
```
git clone git@github.com:vkuznet/WorkQueue.git
cd WorkQueue
make
```

### Runnign the service
```
./workqueue -config config.json
```

### Configuration
Here is an example of configuration file

```
{
    "MetricsFile": "metrics.log",
    "MetricsInterval": 60,
    "Workers": 10,
    "QueueSize": 10,
    "FetchInteral": 60,
    "RequestType": "new",
    "CouchURL": "http://127.0.0.1:5984/",
    "Port": 8888
}
```
