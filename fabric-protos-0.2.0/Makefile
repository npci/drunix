# Adapted from https://github.com/bufbuild/buf-example/blob/main/Makefile

SHELL := /usr/bin/env bash -o pipefail

# This controls the location of the cache.
PROJECT := drunix-protos

# This controls the remote HTTPS git location to compare against for breaking changes in CI.
#
# Most CI providers only clone the branch under test and to a certain depth, so when
# running buf breaking in CI, it is generally preferable to compare against
# the remote repository directly.
#
# Basic authentication is available, see https://buf.build/docs/inputs#https for more details.
HTTPS_GIT := https://github.com/npci/drunix-protos.git

# This controls the remote SSH git location to compare against for breaking changes in CI.
#
# CI providers will typically have an SSH key installed as part of your setup for both
# public and private repositories. Buf knows how to look for an SSH key at ~/.ssh/id_rsa
# and a known hosts file at ~/.ssh/known_hosts or /etc/ssh/known_hosts without any further
# configuration. We demo this with CircleCI.
#
# See https://buf.build/docs/inputs#ssh for more details.
SSH_GIT := ssh://git@github.com/npci/drunix-protos.git

# This controls the version of buf to install and use.
BUF_VERSION := 1.1.1
# If true, Buf is installed from source instead of from releases
BUF_INSTALL_FROM_SOURCE := false

PROTOC_VERSION := 3.19.4
PROTOC_GEN_DOC_VERSION := 1.5.1
PROTOC_GEN_GO_VERSION := v1.28.0
PROTOC_GEN_GO_GRPC_VERSION := v1.2.0
PROTOC_GEN_GRPC_JAVA_VERSION := 1.45.1
GRPC_TOOLS_VERSION := 1.11.2
TS_PROTOC_GEN_VERSION := 0.15.0

# This is the commit hash for the https://github.com/googleapis/googleapis repo
GRPC_STATUS_VERSION := 047d3a8ac7f75383855df0166144f891d7af08d9
GRPC_STATUS_PROTO := google/rpc/status.proto

### Everything below this line is meant to be static, i.e. only adjust the above variables. ###

UNAME_OS := $(shell uname -s)
UNAME_ARCH := $(shell uname -m)
ifeq ($(UNAME_OS),Darwin)
	PLATFORM := osx
endif
ifeq ($(UNAME_OS),Linux)
	PLATFORM := linux
endif
# Buf will be cached to ~/.cache/buf-example.
CACHE_BASE := $(HOME)/.cache/$(PROJECT)
# This allows switching between i.e a Docker container and your local setup without overwriting.
CACHE := $(CACHE_BASE)/$(UNAME_OS)/$(UNAME_ARCH)
# The location where buf will be installed.
CACHE_BIN := $(CACHE)/bin
# Marker files are put into this directory to denote the current version of binaries that are installed.
CACHE_VERSIONS := $(CACHE)/versions

# Update the $PATH so we can use buf directly
export PATH := $(abspath $(CACHE_BIN)):$(PATH)
# Update GOBIN to point to CACHE_BIN for source installations
export GOBIN := $(abspath $(CACHE_BIN))
# This is needed to allow versions to be added to Golang modules with go get
export GO111MODULE := on

# BUF points to the marker file for the installed version.
#
# If BUF_VERSION is changed, the binary will be re-downloaded.
BUF := $(CACHE_VERSIONS)/buf/$(BUF_VERSION)
$(BUF):
	@rm -f $(CACHE_BIN)/buf
	@mkdir -p $(CACHE_BIN)
ifeq ($(BUF_INSTALL_FROM_SOURCE),true)
	$(eval BUF_TMP := $(shell mktemp -d))
	cd $(BUF_TMP); go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
	@rm -rf $(BUF_TMP)
else
	curl -sSL \
		"https://github.com/bufbuild/buf/releases/download/v$(BUF_VERSION)/buf-$(UNAME_OS)-$(UNAME_ARCH)" \
		-o "$(CACHE_BIN)/buf"
	chmod +x "$(CACHE_BIN)/buf"
endif
	@rm -rf $(dir $(BUF))
	@mkdir -p $(dir $(BUF))
	@touch $(BUF)

# PROTOC points to the marker file for the installed version.
#
# If PROTOC_VERSION is changed, the binary will be re-downloaded.
PROTOC := $(CACHE_VERSIONS)/protoc/$(PROTOC_VERSION)
$(PROTOC):
	@rm -f $(CACHE_BIN)/protoc
	@mkdir -p $(CACHE_BIN)
	$(eval PROTOC_TMP := $(shell mktemp -d))
	curl -sSL \
		"https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PLATFORM)-$(UNAME_ARCH).zip" \
		-o "$(PROTOC_TMP)/protoc.zip"
	unzip -o "$(PROTOC_TMP)/protoc.zip" -d "$(CACHE)" bin/protoc
	unzip -o "$(PROTOC_TMP)/protoc.zip" -d "$(CACHE)" include/*
	@rm -rf $(PROTOC_TMP)
	chmod +x "$(CACHE_BIN)/protoc"
	@rm -rf $(dir $(PROTOC))
	@mkdir -p $(dir $(PROTOC))
	@touch $(PROTOC)

# PROTOC_GEN_DOC points to the marker file for the installed version.
#
# If PROTOC_GEN_DOC_VERSION is changed, the binary will be re-downloaded.
PROTOC_GEN_DOC := $(CACHE_VERSIONS)/protoc-gen-doc/$(PROTOC_GEN_DOC_VERSION)
$(PROTOC_GEN_DOC):
	@rm -f $(CACHE_BIN)/protoc-gen-doc
	@mkdir -p $(CACHE_BIN)
	$(eval PROTOC_GEN_DOC_TMP := $(shell mktemp -d))
	cd $(PROTOC_GEN_DOC_TMP); go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@v$(PROTOC_GEN_DOC_VERSION)
	@rm -rf $(PROTOC_GEN_DOC_TMP)
	@rm -rf $(dir $(PROTOC_GEN_DOC))
	@mkdir -p $(dir $(PROTOC_GEN_DOC))
	@touch $(PROTOC_GEN_DOC)

# PROTOC_GEN_GO points to the marker file for the installed version.
#
# If PROTOC_GEN_GO_VERSION is changed, the binary will be re-downloaded.
PROTOC_GEN_GO := $(CACHE_VERSIONS)/protoc-gen-go/$(PROTOC_GEN_GO_VERSION)
$(PROTOC_GEN_GO):
	@rm -f $(CACHE_BIN)/protoc-gen-go
	@mkdir -p $(CACHE_BIN)
	$(eval PROTOC_GEN_GO_TMP := $(shell mktemp -d))
	cd $(PROTOC_GEN_GO_TMP); go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@rm -rf $(PROTOC_GEN_GO_TMP)
	@rm -rf $(dir $(PROTOC_GEN_GO))
	@mkdir -p $(dir $(PROTOC_GEN_GO))
	@touch $(PROTOC_GEN_GO)

# PROTOC_GEN_GO_GRPC points to the marker file for the installed version.
#
# If PROTOC_GEN_GO_GRPC_VERSION is changed, the binary will be re-downloaded.
PROTOC_GEN_GO_GRPC := $(CACHE_VERSIONS)/protoc-gen-go-grpc/$(PROTOC_GEN_GO_GRPC_VERSION)
$(PROTOC_GEN_GO_GRPC):
	@rm -f $(CACHE_BIN)/protoc-gen-go-grpc
	@mkdir -p $(CACHE_BIN)
	$(eval PROTOC_GEN_GO_GRPC_TMP := $(shell mktemp -d))
	cd $(PROTOC_GEN_GO_GRPC_TMP); go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@rm -rf $(PROTOC_GEN_GO_GRPC_TMP)
	@rm -rf $(dir $(PROTOC_GEN_GO_GRPC))
	@mkdir -p $(dir $(PROTOC_GEN_GO_GRPC))
	@touch $(PROTOC_GEN_GO_GRPC)

# PROTOC_GEN_GRPC_JAVA points to the marker file for the installed version.
#
# If PROTOC_GEN_GRPC_JAVA_VERSION is changed, the binary will be re-downloaded.
PROTOC_GEN_GRPC_JAVA := $(CACHE_VERSIONS)/protoc-gen-grpc-java/$(PROTOC_GEN_GRPC_JAVA_VERSION)
$(PROTOC_GEN_GRPC_JAVA):
	@rm -f $(CACHE_BIN)/protoc-gen-grpc-java
	@mkdir -p $(CACHE_BIN)
	curl -sSL \
		"https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/$(PROTOC_GEN_GRPC_JAVA_VERSION)/protoc-gen-grpc-java-$(PROTOC_GEN_GRPC_JAVA_VERSION)-$(PLATFORM)-$(UNAME_ARCH).exe" \
		-o "$(CACHE_BIN)/protoc-gen-grpc-java"
	chmod +x "$(CACHE_BIN)/protoc-gen-grpc-java"
	@rm -rf $(dir $(PROTOC_GEN_GRPC_JAVA))
	@mkdir -p $(dir $(PROTOC_GEN_GRPC_JAVA))
	@touch $(PROTOC_GEN_GRPC_JAVA)

# GRPC_TOOLS points to the marker file for the installed version.
#
# If GRPC_TOOLS_VERSION is changed, the binary will be re-downloaded.
GRPC_TOOLS := $(CACHE_VERSIONS)/grpc-tools/$(GRPC_TOOLS_VERSION)
$(GRPC_TOOLS):
	npm uninstall -g --prefix=$(CACHE) grpc-tools
	@mkdir -p $(CACHE_BIN)
	npm install -g --prefix=$(CACHE) grpc-tools@$(GRPC_TOOLS_VERSION)
	@rm -rf $(dir $(GRPC_TOOLS))
	@mkdir -p $(dir $(GRPC_TOOLS))
	@touch $(GRPC_TOOLS)

# TS_PROTOC_GEN points to the marker file for the installed version.
#
# If TS_PROTOC_GEN_VERSION is changed, the binary will be re-downloaded.
TS_PROTOC_GEN := $(CACHE_VERSIONS)/ts-protoc-gen/$(TS_PROTOC_GEN_VERSION)
$(TS_PROTOC_GEN):
	npm uninstall -g --prefix=$(CACHE) ts-protoc-gen
	@mkdir -p $(CACHE_BIN)
	npm install -g --prefix=$(CACHE) ts-protoc-gen@$(TS_PROTOC_GEN_VERSION)
	@rm -rf $(dir $(TS_PROTOC_GEN))
	@mkdir -p $(dir $(TS_PROTOC_GEN))
	@touch $(TS_PROTOC_GEN)

.DEFAULT_GOAL := all

.PHONY: all
all: lint javabindings nodebindings

# deps allows us to install deps without running any checks.

.PHONY: deps
deps: $(BUF) $(PROTOC) $(PROTOC_GEN_DOC) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC) $(PROTOC_GEN_GRPC_JAVA) $(GRPC_TOOLS) $(TS_PROTOC_GEN)

.PHONY: lint
lint: https

# local is what we run when testing locally.
# This does breaking change detection against our local git repository.

.PHONY: local
local: $(BUF) $(PROTOC)
	buf lint
	buf breaking --against '.git#branch=0.2.x'

# https is what we run when testing in most CI providers.
# This does breaking change detection against our remote HTTPS git repository.

.PHONY: https
https: $(BUF) $(PROTOC)
	buf lint
	buf breaking --against "$(HTTPS_GIT)#branch=0.2.x"

# ssh is what we run when testing in CI providers that provide ssh public key authentication.
# This does breaking change detection against our remote HTTPS ssh repository.
# This is especially useful for private repositories.

.PHONY: ssh
ssh: $(BUF) $(PROTOC)
	buf lint
	buf breaking --against "$(SSH_GIT)#branch=0.2.x"

$(GRPC_STATUS_PROTO):
	@mkdir -p $(dir $(GRPC_STATUS_PROTO))
	curl -sSL \
		"https://raw.githubusercontent.com/googleapis/googleapis/$(GRPC_STATUS_VERSION)/$(GRPC_STATUS_PROTO)" \
		-o "$(GRPC_STATUS_PROTO)"

.PHONY: genprotos
genprotos: $(BUF) $(PROTOC) $(PROTOC_GEN_DOC) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC) $(PROTOC_GEN_GRPC_JAVA) $(GRPC_TOOLS) $(TS_PROTOC_GEN) $(GRPC_STATUS_PROTO)
	buf generate --template buf.gen.yaml

.PHONY: javabindings
javabindings: genprotos
	cd bindings/java && mvn install -DskipTests

.PHONY: nodebindings
nodebindings: genprotos
	./scripts/generate_node_indexes.sh bindings/node/src
	cd bindings/node && npm ci && npm run compile

# clean deletes any files not checked in and the cache for all platforms.

.PHONY: clean
clean:
	git clean -xdf

.PHONY: cleandep
cleandep:
	rm -rf $(CACHE_BASE)

# For updating this repository

.PHONY: updateversion
updateversion:
ifndef VERSION
	$(error "VERSION must be set")
else
ifeq ($(UNAME_OS),Darwin)
	sed -i '' "s/BUF_VERSION := [0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/BUF_VERSION := $(VERSION)/g" Makefile
else
	sed -i "s/BUF_VERSION := [0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/BUF_VERSION := $(VERSION)/g" Makefile
endif
endif
