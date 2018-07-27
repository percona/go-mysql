all: test

PACKAGES := $(shell go list ./... | grep -v vendor)

init:
	go get -u github.com/AlekSi/gocoverutil

install:
	go install -v $(PACKAGES)

test:
	go test -v -race $(PACKAGES)

test-travis:
	gocoverutil test -v -race $(PACKAGES)
