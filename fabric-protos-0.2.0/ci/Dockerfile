# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

ARG DEBIAN_BASE=buster
ARG GO_VER=1.16.7
ARG PROTOC_VER=1.3.2
ARG PROTOTOOL_VER=1.9.0

FROM golang:${GO_VER}-${DEBIAN_BASE} as golang
WORKDIR /tools
ARG PROTOC_VER
RUN git clone -q -c advice.detachedHead=false -b v${PROTOC_VER} --depth 1 https://github.com/golang/protobuf
WORKDIR /tools/protobuf
RUN GOBIN=/usr/local/bin go install ./protoc-gen-go

FROM debian:${DEBIAN_BASE} as prototool
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
ARG PROTOTOOL_VER=1.8.0
RUN curl -sL -o /usr/local/bin/prototool https://github.com/uber/prototool/releases/download/v${PROTOTOOL_VER}/prototool-Linux-x86_64 && chmod +x /usr/local/bin/prototool

FROM golang:${GO_VER}-${DEBIAN_BASE}
RUN apt-get update && apt-get install -y git protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY --from=golang /usr/local/bin/protoc-gen-go /usr/local/bin/
COPY --from=prototool /usr/local/bin/prototool /usr/local/bin/
