.PHONY: build prepare reverse all full gotool clean help

CMDPATH="./cmd"
BINARYPATH="bin/transferdb"
CONFIGPATH="./conf/config.toml"

REPO    := github.com/wentaojin/transferdb

GOOS    := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH  := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
GOENV   := GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=x86_64-linux-musl-gcc CGO_LDFLAGS="-static"
GO      := $(GOENV) go
GOBUILD := $(GO) build
GORUN   := $(GO) run
SHELL   := /usr/bin/env bash

COMMIT  := $(shell git describe --always --no-match --tags --dirty="-dev")
BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITREF  := $(shell git rev-parse --abbrev-ref HEAD)


LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/pkg/config.Version=$(COMMIT)"
LDFLAGS += -X "$(REPO)/pkg/config.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REPO)/pkg/config.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REPO)/pkg/config.GitBranch=$(GITREF)"


build: clean gotool
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH) $(CMDPATH)

prepare: gotool
	$(GORUN) -race $(CMDPATH) --config $(CONFIGPATH) --mode prepare

reverse: gotool
	$(GORUN) -race $(CMDPATH) --config $(CONFIGPATH) --mode reverse

all: gotool
	$(GORUN) -race $(CMDPATH) --config $(CONFIGPATH) --mode all

full: gotool
	$(GORUN) -race $(CMDPATH) --config $(CONFIGPATH) --mode full

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
