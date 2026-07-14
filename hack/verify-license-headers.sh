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

file_patterns=(
  '\.go$'
  '\.py$'
  '\.sh$'
  '\.yaml$'
  '\.yml$'
  '\.proto$'
  'Dockerfile'
)

pathspec=(".")
for exclude in "${excluded_paths[@]}"; do
  pathspec+=(":(exclude)${exclude}")
done

printf -v regex_pattern "%s|" "${file_patterns[@]}"
regex_pattern="${regex_pattern%|}"

echo ">>> Validating Apache 2.0 and Google LLC copyright headers..."

failed_files=0

mapfile -t files < <(git ls-files -- "${pathspec[@]}" | grep -E "${regex_pattern}" || true)

target_files=()
for f in "${files[@]}"; do
  if [ -f "$f" ]; then
    target_files+=("$f")
  fi
done


if [ ${#target_files[@]} -gt 0 ]; then
  failed_files_list=$(awk '
    FNR == 1 {
      if (NR > 1 && (apache==0 || copy==0 || google==0)) {
        print prev_file
      }
      apache=0; copy=0; google=0; prev_file=FILENAME
    }
    FNR <= 30 {
      if (/Licensed under the Apache License, Version 2.0/) apache=1
      if (/Copyright/) copy=1
      if (/Google LLC/) google=1
      if (apache && copy && google) nextfile
    }
    FNR == 30 { nextfile }
    END {
      if (NR > 0 && (apache==0 || copy==0 || google==0)) {
        print prev_file
      }
    }
  ' "${target_files[@]}")

  if [[ -n "$failed_files_list" ]]; then
    while IFS= read -r file; do
      echo "Missing required license or Google LLC attribution: ${file}" >&2
    done <<< "$failed_files_list"
    failed_files=$(echo "$failed_files_list" | wc -l)
  fi
fi

if [ "$failed_files" -gt 0 ]; then
  echo ">>> Validation failed: ${failed_files} file(s) lack the correct Google LLC Apache 2.0 license header." >&2
  echo ">>> License headers may be added using command:"
  echo "addlicense -c \"Google LLC\" -l apache -ignore \"\$(git rev-parse --show-toplevel)/vendor/**\" -ignore \"\$(git rev-parse --show-toplevel)/cluster-autoscaler/vendor/**\" \$(git rev-parse --show-toplevel)/."
  echo ">>> Tool reference: https://github.com/google/addlicense"
  exit 1
else
  echo ">>> All files contain the required Google LLC Apache 2.0 header."
fi
