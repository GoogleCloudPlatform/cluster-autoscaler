#!/usr/bin/env bash
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


CA_ROOT="$(git rev-parse --show-toplevel)"

command -v controller-gen >/dev/null 2>&1 || { echo >&2 "controller-gen should be available in PATH, consider installing using 'make gen-clients-init'"; exit 1; }

if [ ! -d "$CA_ROOT" ]; then
  echo "Unable to find ${CA_ROOT} folder"
  exit 1
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <package>"
  exit 1
fi

echo -e "Going to temporary swap code-generator vendor (context - b/354648045)"
read -rp "Continue? (y/n): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted." >&2
    exit 0
fi

PKG=$1
CODEGEN_PKG="${CA_ROOT}/vendor/k8s.io/code-generator"
CODEGEN_REPO="https://github.com/kubernetes/code-generator.git"
CODEGEN_VERSION="kubernetes-1.30.3"

# Swapping code-gen vendored package context to the last version compliant with 1.27 - 1.30
# Once 1.31 will become the oldest supported version we are able to remove that
# because https://pkg.go.dev/k8s.io/client-go/gentype package will become available to all versions
rm -rf "${CODEGEN_PKG}"
function revert_vendor_patch() {
  rm -rf "${CODEGEN_PKG}"
  git checkout -- "${CODEGEN_PKG}"
  exit
}
trap revert_vendor_patch EXIT SIGINT
git clone --branch "${CODEGEN_VERSION}" --single-branch "${CODEGEN_REPO}" "${CODEGEN_PKG}"

. "${CODEGEN_PKG}/kube_codegen.sh"

BOILERPLATE_FILE="${CA_ROOT}/hack/boilerplate.go.txt"
YAML_BOILERPLATE_FILE="${CA_ROOT}/hack/boilerplate.yaml.txt"
K8S_OUTPUT_PKG_BASE="k8s.io/gke-autoscaling/cluster-autoscaler"
COMMON_CRD_OUTPUT_DIR="${CA_ROOT}/deploy/"

if [ "${PKG}" = "machineconfig" ]; then
    echo "INFO: Configuring for 'apis' module..."
    MODULE_SUBPATH="apis/machineconfig"
else
    echo "INFO: Configuring for '${PKG}' package..."
    MODULE_SUBPATH="pkg/${PKG}"
fi

TARGET_API_BASE_PATH="${CA_ROOT}/${MODULE_SUBPATH}"
TARGET_CLIENT_OUTPUT_PKG="${K8S_OUTPUT_PKG_BASE}/${MODULE_SUBPATH}/client"

echo "INFO: Cleaning client directory: ${TARGET_API_BASE_PATH}/client"
rm -rf "${TARGET_API_BASE_PATH}/client"

if [ "${PKG}" = "machineconfig" ]; then
    TARGET_API_FULL_PATH="${TARGET_API_BASE_PATH}"
    pushd "${TARGET_API_BASE_PATH}" >/dev/null
else
    TARGET_API_FULL_PATH="${TARGET_API_BASE_PATH}/apis"
fi

echo "INFO: Generating helpers for ${TARGET_API_FULL_PATH}..."
kube::codegen::gen_helpers "${TARGET_API_FULL_PATH}" --boilerplate "${BOILERPLATE_FILE}"

echo "INFO: Generating clients for ${TARGET_API_FULL_PATH}..."
kube::codegen::gen_client "${TARGET_API_FULL_PATH}" \
    --output-dir "${TARGET_API_BASE_PATH}/client" \
    --output-pkg "${TARGET_CLIENT_OUTPUT_PKG}" \
    --with-applyconfig \
    --with-watch \
    --boilerplate "${BOILERPLATE_FILE}"

echo "INFO: Generating CRD manifests from ${TARGET_API_FULL_PATH}..."
controller-gen \
  crd:generateEmbeddedObjectMeta=true,headerFile="${YAML_BOILERPLATE_FILE}" \
  paths="${TARGET_API_FULL_PATH}/..." \
  output:crd:dir="${COMMON_CRD_OUTPUT_DIR}"

if [ "${PKG}" = "machineconfig" ]; then
    popd >/dev/null
    echo "INFO: Running go mod tidy && go mod vendor for 'apis'..."
    go mod tidy && go mod vendor
fi
