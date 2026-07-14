#!/bin/bash
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

# This script installs protoc (Protocol Buffer Compiler) for the current or specified architecture.
# It verifies the download using hardcoded SHA256 checksums.

PROTOC_VERSION=${1:-"21.12"}
INSTALL_DIR=${2:-"/usr/local"}
ARCH_OVERRIDE=${3:-""}

ARCH="${ARCH_OVERRIDE}"
if [[ -z "${ARCH}" ]]; then
  ARCH=$(uname -m)
fi

case "${ARCH}" in
  x86_64|amd64|linux/amd64)
    PROTOC_ARCH="x86_64"
    PROTOC_SHA256="3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8"
    ;;
  aarch64|arm64|linux/arm64)
    PROTOC_ARCH="aarch_64"
    PROTOC_SHA256="2dd17f75d66a682640b136e31848da9fb2eefe68d55303baf8b32617374f6711"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH}"
    exit 1
    ;;
esac

PROTOC_ZIP="protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"

# Use a temporary directory for downloading
TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT
cd "${TMP_DIR}"

echo "Downloading protoc ${PROTOC_VERSION} for ${PROTOC_ARCH}..."
curl -fLO "${PROTOC_URL}"
echo "${PROTOC_SHA256}  ${PROTOC_ZIP}" | sha256sum -c -

unzip -o "${PROTOC_ZIP}" -d "${INSTALL_DIR}" bin/protoc
unzip -o "${PROTOC_ZIP}" -d "${INSTALL_DIR}" 'include/*'

echo "Successfully installed protoc to ${INSTALL_DIR}"
