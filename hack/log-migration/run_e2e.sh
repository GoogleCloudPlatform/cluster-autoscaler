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

set -e

echo "Building migration tool..."
go build -o migration_bin .
rm -rf e2e/testdata/actual
cp -r e2e/testdata/before e2e/testdata/actual

echo "Running migration tool on testdata..."
(cd e2e/testdata/actual && ../../../migration_bin -path="./..." -migrate-logs)

echo "Comparing actual outcome with expected outcome..."
diff -ruN e2e/testdata/expected/ e2e/testdata/actual/ > e2e/test_diff.patch || (echo "E2E Test Failed! See e2e/test_diff.patch" && exit 1)

echo "E2E Test Passed!"
echo "Verifying compilability of generated project..."
(cd e2e/testdata/actual && go build ./...)
