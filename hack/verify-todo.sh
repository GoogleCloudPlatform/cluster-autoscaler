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

SCRIPT_ROOT=$(dirname "${BASH_SOURCE}")/..
cd "${SCRIPT_ROOT}"

bad_todos=$(git grep -n "//.*TODO" . \
  | grep -v "^vendor/" \
  | grep -v "^apis/machineconfig/client/" \
  | grep -v "^hack/verify-todo.sh" \
  | grep -v -E "//.*TODO\((b|go)/[^)]+\):|//.*TODO:" || true)

if [[ -n "${bad_todos}" ]]; then
  echo "!!! Malformed TODO comments found:"
  echo "${bad_todos}"
  echo ""
  echo "The correct format is one of:"
  echo "// TODO(b/XYZ): comment"
  echo "// TODO(go/XYZ): comment"
  echo "// TODO: comment"
  exit 1
fi
