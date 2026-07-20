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

SCRIPT_ROOT=$(dirname "${BASH_SOURCE}")/..
cd "${SCRIPT_ROOT}"

excluded_paths=(
  "vendor"
  "hack/verify-internal-links.sh"
  "hack/internal"
  "proto"
  "test/frankenstein"
  "kokoro"
)

grep_args=(
  -e "corp.google.com"
  -e "googleplex.com"
  -e "borg.google.com"
  -e "prod.google.com"
  -e "c.googlers.com"
  -e "/bns/"
  -e "/abns/"
  -e "/ls/"
  -e "/cfs/"
  -e "/cns/"
  -e "/x20/"
  -e "/binfs/"
)

pathspec=(".")
for exclude in "${excluded_paths[@]}"; do
  pathspec+=(":(exclude)${exclude}")
done

leaks=$(git grep -n -F "${grep_args[@]}" -- "${pathspec[@]}" || true)

if [[ -n "${leaks}" ]]; then
  echo ">>> Internal Google hostnames, domain names, or internal service paths found:"
  echo "${leaks}"
  echo
  echo "The primary concern is to avoid exposing internal network addresses, hostnames, or service paths."
  echo "Note: Common internal shortlinks (e.g., go/, b/, cl/) are not targeted by this check."
  echo "Consider avoiding using internal hostnames/domains or wrapping them into a go link."
  exit 1
fi

echo ">>> No internal hostnames, domain names, or internal service paths found."
