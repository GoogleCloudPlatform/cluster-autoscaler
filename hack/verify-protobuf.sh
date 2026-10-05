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

cd "${PROJECT_ROOT}"

mapfile -t PROTO_FILES < <(find . -name "*.proto" -not -path "*/vendor/*")

if [[ ${#PROTO_FILES[@]} -eq 0 ]]; then
  echo ">>> No .proto files found."
  exit 1
fi

echo ">>> Protoc version: $(protoc --version)"
echo ">>> Protoc-gen-go version: $(protoc-gen-go --version)"

# Generate into a temporary directory and compare with the working tree instead of regenerating
# in place.
GEN_DIR=$(mktemp -d)
trap 'rm -rf "${GEN_DIR}"' EXIT

# We are using --go_opt=paths=source_relative to guarantee a specific file path resolution
# strategy as this holds for all the existing proto messages in the repository. If this
# assumption needs to be broken for whatever reason, this script needs to be changed to
# locate the generated files in other ways.
for proto in "${PROTO_FILES[@]}"; do
  protoc "${proto}" --go_out="${GEN_DIR}" --go_opt=paths=source_relative
done

# PROTO_FILES holds paths relative to PROJECT_ROOT (e.g. ./pkg/foo/foo.proto), because `find .`
# runs after `cd "${PROJECT_ROOT}"`. With paths=source_relative, protoc mirrors these relative
# paths under --go_out, so prefixing GEN_DIR yields the generated counterpart directly and the
# working-tree file is resolved against PROJECT_ROOT. If PROTO_FILES ever contains absolute
# paths, this mapping breaks and needs to be updated.
MODIFIED_COUNT=0
for proto in "${PROTO_FILES[@]}"; do
  pb_file="${proto%.proto}.pb.go"
  generated_file="${GEN_DIR}/${pb_file}"

  if [[ ! -e "${generated_file}" ]]; then
    printf 'Error: protoc did not generate "%s" for "%s"\n' "${pb_file}" "${proto}" >&2
    exit 1
  fi

  if [[ ! -e "${pb_file}" ]]; then
    echo ">>> ${pb_file} is missing from the working tree"
    MODIFIED_COUNT=$((MODIFIED_COUNT + 1))
  elif ! cmp -s "${generated_file}" "${pb_file}"; then
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