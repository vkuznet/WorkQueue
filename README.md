# WorkQueue
Go-implementation of CMS DMWM WorkQueue

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
