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

cd "$(dirname "${BASH_SOURCE[0]}")/.."

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# It's not possible to install helm chart as is because it requires a non-placeholder version
# for experiment setup, in case user attempts to install chart as is - chart raises errors
# and that pollutes helm lint outputs
helm package --destination "${tmp_dir}" --version 0.0.0 --app-version 0.0.0 charts/cluster-autoscaler
helm lint "${tmp_dir}"/cluster-autoscaler-0.0.0.tgz
helm unittest charts/cluster-autoscaler
