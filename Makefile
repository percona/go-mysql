all: test

PACKAGES := $(shell go list ./... | grep -v vendor)

init:
	go get -u github.com/AlekSi/gocoverutil
	go get -u gopkg.in/alecthomas/gometalinter.v1
	gometalinter.v1 --install

install:
	go install -v $(PACKAGES)
	go test -v -i $(PACKAGES)

install-race:
	go install -v -race $(PACKAGES)
	go test -v -race -i $(PACKAGES)

test: install
	go test -v $(PACKAGES)

test-race: install-race
	go test -v -race $(PACKAGES)

cover: install
	gocoverutil test -v $(PACKAGES)

check: install
	-gometalinter.v1 --tests --skip=api --deadline=180s ./...
