help:                           ## Display this help message.
	@echo "Please use \`make <target>\` where <target> is one of:"
	@grep '^[a-zA-Z]' $(MAKEFILE_LIST) | \
		awk -F ':.*?## ' 'NF==2 {printf "  %-26s%s\n", $$1, $$2}'

init:                           ## Installs tools to $GOPATH/bin (which is expected to be in $PATH).
	curl https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin

	go install ./vendor/golang.org/x/tools/cmd/goimports

	go test -i ./...
	go test -race -i ./...

TEST_FLAGS ?= -timeout=20s

test:                           ## Run tests.
	go test $(TEST_FLAGS) ./...

test-race:                      ## Run tests with race detector.
	go test $(TEST_FLAGS) -race ./...

test-cover:                     ## Run tests and collect per-package coverage information.
	go test $(TEST_FLAGS) -coverprofile=cover.out -covermode=count ./...

test-crosscover:                ## Run tests and collect cross-package coverage information.
	go test $(TEST_FLAGS) -coverprofile=crosscover.out -covermode=count -coverpkg=./... ./...

check:                          ## Run required checkers and linters.
	go run .github/check-license.go

FILES = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

format:                         ## Format source code.
	gofmt -w -s $(FILES)
	goimports -local github.com/yehornaumenko/go-mysql -l -w $(FILES)

.PHONY: test
