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

KUBE_ROOT=$(dirname "${BASH_SOURCE}")/..
cd "${KUBE_ROOT}"

excluded_paths=(
  "vendor"
  "kokoro"
  "proto"
  "test/frankenstein"
  # This folder is auto-generated from google3 files and is not exported to OSS (reference: go/releasing).
  "pkg/internalclients/uas/client/proto/google/cloud/autoscaling/v1alpha1"
)

# Files and directories (e.g. copied from upstream sigs.k8s.io/cluster-autoscaler) whose copyright
# headers must be attributed to "The Kubernetes Authors" instead of "Google LLC".
kubernetes_authors_paths=(
  # TODO(b/571005331): Temporary copies from upstream sigs.k8s.io/cluster-autoscaler retaining "Copyright The Kubernetes Authors."
  "pkg/processors/capacitybuffers/autoscaling_status_processor.go"
  "pkg/processors/capacitybuffers/autoscaling_status_processor_test.go"
  "pkg/processors/capacitybuffers/ready_replicas_controller.go"
  "pkg/processors/capacitybuffers/ready_replicas_controller_test.go"
  # GCE cloud provider synced from upstream sigs.k8s.io/cluster-autoscaler.
  "pkg/cloudprovider/gce"
)

file_patterns=(
  '\.go$'
  '\.py$'
  '\.sh$'
  '\.yaml$'
  '\.yml$'
  '\.proto$'
  'Dockerfile'
)

printf -v regex_pattern "%s|" "${file_patterns[@]}"
regex_pattern="${regex_pattern%|}"

# Prints the tracked files matching the given pathspecs and file_patterns (except excluded_paths)
# that lack the Apache 2.0 license or the given copyright holder within their first 30 lines.
# Arguments: <copyright holder> [<pathspec>...]
find_files_missing_header() {
  local holder="$1"
  shift
  # Without any pathspec git ls-files would list all files.
  if [ $# -eq 0 ]; then
    return 0
  fi

  local pathspec=("$@")
  local exclude
  for exclude in "${excluded_paths[@]}"; do
    pathspec+=(":(exclude)${exclude}")
  done

  local files
  mapfile -t files < <(git ls-files -- "${pathspec[@]}" | grep -E "${regex_pattern}" || true)

  local target_files=()
  local f
  for f in "${files[@]}"; do
    if [ -f "$f" ]; then
      target_files+=("$f")
    fi
  done

  if [ ${#target_files[@]} -eq 0 ]; then
    return 0
  fi

  awk -v holder="${holder}" '
    FNR == 1 {
      if (NR > 1 && (apache==0 || copy==0 || attributed==0)) {
        print prev_file
      }
      apache=0; copy=0; attributed=0; prev_file=FILENAME
    }
    FNR <= 30 {
      if (/Licensed under the Apache License, Version 2.0/) apache=1
      if (/Copyright/) copy=1
      if (index($0, holder)) attributed=1
      if (apache && copy && attributed) nextfile
    }
    FNR == 30 { nextfile }
    END {
      if (NR > 0 && (apache==0 || copy==0 || attributed==0)) {
        print prev_file
      }
    }
  ' "${target_files[@]}"
}

# Reports each of the given newline-separated files as missing the license or the given copyright
# holder and increments failed_files accordingly.
# Arguments: <copyright holder> <files>
report_failed_files() {
  local holder="$1"
  local file
  while IFS= read -r file; do
    if [[ -n "$file" ]]; then
      echo "Missing required license or ${holder} attribution: ${file}" >&2
      failed_files=$((failed_files + 1))
    fi
  done <<< "$2"
}

echo ">>> Validating Apache 2.0 and Google LLC / The Kubernetes Authors copyright headers..."

google_llc_pathspec=(".")
for path in "${kubernetes_authors_paths[@]}"; do
  google_llc_pathspec+=(":(exclude)${path}")
done

google_llc_failed_list=$(find_files_missing_header "Google LLC" "${google_llc_pathspec[@]}")
kubernetes_authors_failed_list=$(find_files_missing_header "The Kubernetes Authors" "${kubernetes_authors_paths[@]}")

failed_files=0
report_failed_files "Google LLC" "${google_llc_failed_list}"
report_failed_files "The Kubernetes Authors" "${kubernetes_authors_failed_list}"

if [ "$failed_files" -gt 0 ]; then
  echo ">>> Validation failed: ${failed_files} file(s) lack the correct Apache 2.0 license header." >&2
  echo ">>> License headers may be added using command(s):"
  # Printed first, as the Google LLC command below would also add headers to the files listed here.
  if [[ -n "${kubernetes_authors_failed_list}" ]]; then
    mapfile -t kubernetes_authors_failed_files <<< "${kubernetes_authors_failed_list}"
    printf "addlicense -c \"The Kubernetes Authors\" -l apache"
    printf " \$(git rev-parse --show-toplevel)/%s" "${kubernetes_authors_failed_files[@]}"
    echo
  fi
  if [[ -n "${google_llc_failed_list}" ]]; then
    echo "addlicense -c \"Google LLC\" -l apache -ignore \"\$(git rev-parse --show-toplevel)/vendor/**\" -ignore \"\$(git rev-parse --show-toplevel)/cluster-autoscaler/vendor/**\" \$(git rev-parse --show-toplevel)/."
  fi
  echo ">>> Tool reference: https://github.com/google/addlicense"
  exit 1
else
  echo ">>> All files contain the required Apache 2.0 license header."
fi
