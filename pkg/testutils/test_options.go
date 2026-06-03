// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	"os"
	"testing"
)

const (
	LongRunningAllowedEnvVar string = "CA_RUN_LONG_TESTS"
	TestForManualRun         string = "CA_MANUAL_TEST"
	RunningAllowedEnvValue   string = "true"
)

// MarkTestLongRunning should be used for long-running unit tests. When called inside a test, it skips it by default,
// unless a CA_RUN_LONG_TESTS=true environment variable is defined (e.g. in a CI setup).
func MarkTestLongRunning(t *testing.T) {
	if val := os.Getenv(LongRunningAllowedEnvVar); val == RunningAllowedEnvValue {
		// Long-running tests are explicitly allowed, don't skip anything.
		return
	}
	t.Skipf("Skipping a long-running test (set a %s=%s env variable to disable skipping long-running tests).", LongRunningAllowedEnvVar, RunningAllowedEnvValue)
}

func MarkTestManual(t *testing.T) {
	if val := os.Getenv(TestForManualRun); val == RunningAllowedEnvValue {
		// Manual tests are explicitly allowed, don't skip anything.
		return
	}
	t.Skipf("Skipping a test for manual run (set a %s=%s env variable to disable skipping manual tests).", TestForManualRun, RunningAllowedEnvValue)
}
