#!/bin/bash
#
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

set -eu -o pipefail
branch="${1:-main}"

# Lint the protocol buffers
prototool lint

[ -d build/fabric-protos-go/.git ] || git clone https://github.com/hyperledger/fabric-protos-go.git build/fabric-protos-go

# Checkout the appropriate branch and remove generated files
(
  cd build/fabric-protos-go
  git show-branch "${branch}" > /dev/null 2>&1 || git branch "${branch}"
  git checkout "${branch}"
  if git ls-remote --heads --exit-code origin "${branch}"; then
    git pull origin "${branch}"
  fi
  git ls-files | grep -vFx -f <(grep -v '^#' < .whitelist) | tr '\n' '\0' | xargs -0 rm -f
  find ./* -type d -empty -delete
)

ci/compile_go_protos.sh
cd build/fabric-protos-go
go mod tidy
go build ./...

git add -A .
git diff --color --cached
