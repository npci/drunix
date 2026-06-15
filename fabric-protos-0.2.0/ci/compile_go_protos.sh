#!/bin/bash
#
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

set -eu -o pipefail

repo="build/fabric-protos-go"

if [ ! -d "$repo" ]; then
  echo "$repo does not exist"
  exit 1
fi

for protos in $(find . -name '*.proto' -exec dirname {} \; | sort -u); do
  protoc "--go_out=plugins=grpc,paths=source_relative:$repo" "$protos"/*.proto
done

