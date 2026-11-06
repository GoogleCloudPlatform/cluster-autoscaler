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

# Verifies that commits produced by Custom Compute Class (CCC) dependency bump
# scripts modify only the expected Go module and compute-class-api vendor paths.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${SCRIPT_ROOT}"

TARGET_COMMIT="HEAD"
for candidate in "${KOKORO_GIT_COMMIT:-}" "${PULL_PULL_SHA:-}"; do
  if [[ -n "${candidate}" ]] && git rev-parse --verify "${candidate}^{commit}" >/dev/null 2>&1; then
    TARGET_COMMIT="${candidate}"
    break
  fi
done

if ! git rev-parse --verify "${TARGET_COMMIT}^{commit}" >/dev/null 2>&1; then
  exit 0
fi

SUBJECT=$(git log -1 --format=%s "${TARGET_COMMIT}")
COMMIT_MSG=$(git log -1 --format=%B "${TARGET_COMMIT}")

IS_CCC_BUMP="no"
if [[ "${SUBJECT}" =~ ^[Bb]ump[[:space:]]+github\.com/[gG]oogle[cC]loud[pP]latform/compute-class-api([[:space:]]|$) ]]; then
  IS_CCC_BUMP="yes"
elif echo "${COMMIT_MSG}" | grep -Eq '(^|[[:space:]])CREATED_WITH=.*(update-ccc\.sh|update_ccc_deps\.sh)'; then
  IS_CCC_BUMP="yes"
fi

if [[ "${IS_CCC_BUMP}" != "yes" ]]; then
  echo ">>> ${TARGET_COMMIT} is not a compute-class-api vendor bump; skipping vendor-path check."
  exit 0
fi

if echo "${COMMIT_MSG}" | grep -Eq '(^|[[:space:]])ALLOW_EXTRA_VENDOR_PATHS=[^[:space:]]+'; then
  echo ">>> ALLOW_EXTRA_VENDOR_PATHS acknowledgment present; skipping strict vendor-path check."
  exit 0
fi

ALLOWED_REGEX='^(go\.mod|go\.sum|vendor/modules\.txt|vendor/github\.com/googlecloudplatform/compute-class-api/.*|hack/internal/oss/go\.mod|hack/internal/oss/go\.sum)$'

DISALLOWED_FILES=()
while IFS= read -r file; do
  if [[ -z "${file}" ]]; then
    continue
  fi
  if [[ ! "${file}" =~ ${ALLOWED_REGEX} ]]; then
    DISALLOWED_FILES+=("${file}")
  fi
done < <(git diff-tree --root --no-commit-id --name-only -r "${TARGET_COMMIT}")

if [[ ${#DISALLOWED_FILES[@]} -gt 0 ]]; then
  echo ">>> Error: compute-class-api vendor bump commit modifies paths outside the allowed vendor set:" >&2
  for file in "${DISALLOWED_FILES[@]}"; do
    echo "  - ${file}" >&2
  done
  echo ">>> Allowed pattern: ${ALLOWED_REGEX}" >&2
  echo ">>> If compute-class-api legitimately added/updated transitive vendor dependencies, add ALLOW_EXTRA_VENDOR_PATHS=<reason> to the commit message (human Code-Review+2 will still be required)." >&2
  exit 1
fi

echo ">>> Verified compute-class-api vendor bump touches only allowed vendor paths."
