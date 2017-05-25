GOPATH:=$(PWD):${GOPATH}
export GOPATH
# flags=-ldflags="-s -w"
flags=-ldflags="-s -w -extldflags -static"

all: build

build:
	go clean; rm -rf pkg WorkQueue*; go build ${flags}

build_all: build build_osx build_linux build_power8 build_arm64

build_osx:
	go clean; rm -rf pkg WorkQueue_osx; GOOS=darwin go build ${flags}
	mv WorkQueue WorkQueue_osx

build_linux:
	go clean; rm -rf pkg WorkQueue_linux; GOOS=linux go build ${flags}
	mv WorkQueue WorkQueue_linux

build_power8:
	go clean; rm -rf pkg WorkQueue_power8; GOARCH=ppc64le GOOS=linux go build ${flags}
	mv WorkQueue WorkQueue_power8

build_arm64:
	go clean; rm -rf pkg WorkQueue_arm64; GOARCH=arm64 GOOS=linux go build ${flags}
	mv WorkQueue WorkQueue_arm64

build_windows:
	go clean; rm -rf pkg WorkQueue.exe; GOARCH=amd64 GOOS=windows go build ${flags}

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test
