.PHONY: build assessO2M assessO2T prepare checkO2M checkO2T checkM2O checkT2O reverseO2M reverseO2T reverseM2O reverseT2O allO2T allO2M fullO2M fullO2T csvO2M csvO2T comapreO2M compareO2T gotool clean help

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

assessO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode assess -source oracle -target mysql

assessO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode assess -source oracle -target tidb

prepare: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode prepare

reverseO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reverse -source oracle -target mysql

reverseM2O: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reverse -source mysql -target oracle

reverseO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reverse -source oracle -target tidb

reverseT2O: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode reverse -source tidb -target oracle

checkO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode check -source oracle -target mysql

checkO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode check -source oracle -target tidb

checkM2O: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode check -source mysql -target oracle

checkT2O: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode check -source tidb -target oracle

allO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode all -source oracle -target mysql

allO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode all -source oracle -target tidb

compareO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode compare -source oracle -target mysql

compareO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode compare -source oracle -target tidb

fullO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode full -source oracle -target tidb

fullO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode full -source oracle -target mysql

csvO2T: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode csv -source oracle -target tidb

csvO2M: gotool
	$(GORUN) $(CMDPATH) --config $(CONFIGPATH) --mode csv -source oracle -target mysql

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
