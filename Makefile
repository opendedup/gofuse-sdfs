PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
TAG ?= "sdfs/mount.sdfs:$(VERSION)"

all: getdeps build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@go get ./...
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps fmt lint

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d pkg/

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=5m --config ./.golangci.yml



# Builds mount.sdfs locally.
build:
	@echo "Building mount.sdfs binary to './mount.sdfs'"
	@go build  -ldflags="-X 'main.Version=$(BRANCH)' -X 'main.BuildDate=$$(date -Ins)'" -o ./mount.sdfs app/* 

# Builds mount.sdfs and installs it to $GOPATH/bin.
install: build
	@echo "Installing mount.sdfs binary to '$(GOPATH)/bin/mount.sdfs'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/mount.sdfs $(GOPATH)/bin/mount.sdfs
	@echo "Installation successful. To learn more, try \"mount.sdfs --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf mount.sdfs
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
