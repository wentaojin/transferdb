.PHONY: build assess prepare check reverseO2M reverseM2O all full csv comapre gotool clean help

CMDPATH="./cmd"
BINARYPATH="bin/transferdb"
CONFIGPATH="./example/config.toml"

REPO    := github.com/wentaojin/transferdb

GOOS    := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH  := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
GOENV   := GO111MODULE=on CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH)
GO      := $(GOENV) go
GOBUILD := $(GO) build
GORUN   := $(GO) run
SHELL   := /usr/bin/env bash

COMMIT  := $(shell git describe --always --no-match --tags --dirty="-dev")
BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITREF  := $(shell git rev-parse --abbrev-ref HEAD)


LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/config.Version=$(COMMIT)"
LDFLAGS += -X "$(REPO)/config.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REPO)/config.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REPO)/config.GitBranch=$(GITREF)"


build: clean gotool
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH) $(CMDPATH)

assess: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode assess

prepare: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode prepare

reverseO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reverseo2m

reverseM2O: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reversem2o

check: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode check

all: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode all

compare: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode compare

full: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode full

csv: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode csv

gotool:
	$(GO) mod tidy

clean:
	@if [ -f ${BINARYPATH} ] ; then rm ${BINARYPATH} ; fi

help:
	@echo "make - 格式化 Go 代码, 并编译生成二进制文件"
	@echo "make build - 编译 Go 代码, 生成二进制文件"
	@echo "make run - 直接运行 Go 代码"
	@echo "make clean - 移除二进制文件和 vim swap files"
	@echo "make gotool - 运行 Go 工具 'mod tidy'"
