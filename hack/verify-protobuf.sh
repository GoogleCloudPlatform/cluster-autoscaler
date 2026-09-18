#!/usr/bin/env bash
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
PROJECT_ROOT="$(readlink -f "${SCRIPT_DIR}/..")"

function getFileHash() {
  local file="$1"

  if [[ ! -e "${file}" ]]; then
    echo "MISSING"
    return 0
  fi

  if [[ ! -r "${file}" ]]; then
    printf 'Error: Cannot read file "%s"\n' "${file}" >&2
    exit 1
  fi

  cksum "${file}" | awk '{print $1 ":" $2}'
}

cd "${PROJECT_ROOT}"

mapfile -t PROTO_FILES < <(find . -name "*.proto" -not -path "*/vendor/*")

if [[ ${#PROTO_FILES[@]} -eq 0 ]]; then
  echo ">>> No .proto files found."
  exit 1
fi

PB_GO_FILES=("${PROTO_FILES[@]/%.proto/.pb.go}")

HASHES_BEFORE=()
for i in "${!PB_GO_FILES[@]}"; do
  HASHES_BEFORE[i]=$(getFileHash "${PB_GO_FILES[i]}")
done

echo ">>> Protoc version: $(protoc --version)"
echo ">>> Protoc-gen-go version: $(protoc-gen-go --version)"

# We are using --go_opt=paths=source_relative to guarantee a specific file path resolution
# strategy as this holds for all the existing proto messages in the repository in case this
# assumption needs to be broken for whatever reason - this script needs to be changed to
# simulate dry runs in other ways
for proto in "${PROTO_FILES[@]}"; do
  protoc "${proto}" --go_out=. --go_opt=paths=source_relative
done

MODIFIED_COUNT=0
for i in "${!PB_GO_FILES[@]}"; do
    pb_file="${PB_GO_FILES[i]}"
    hash_before=${HASHES_BEFORE[i]}
    hash_after=$(getFileHash "${PB_GO_FILES[i]}")

    if [[ "${hash_before}" != "${hash_after}" ]]; then
        echo ">>> ${pb_file} differs from existing working tree"
        MODIFIED_COUNT=$((MODIFIED_COUNT + 1))
    fi
done

if [[ ${MODIFIED_COUNT} -gt 0 ]]; then
  echo ""
  echo "Error: Protobuf files are out of date, ${MODIFIED_COUNT} file(s) changed. Please regenerate with: make compile-proto"
  exit 1
fi

echo ">>> All protobuf generated files are up to date."