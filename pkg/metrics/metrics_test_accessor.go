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

package metrics

import (
	"k8s.io/component-base/metrics/testutil"
)

// GetFlexAdvisorGenerationErrorsCountForTest returns the current count for a given reason (only for tests).
func GetFlexAdvisorGenerationErrorsCountForTest(reason FAGenerationErrorReason) (float64, error) {
	gauge := flexAdvisorGenerationErrors.WithLabelValues(string(reason))
	return testutil.GetCounterMetricValue(gauge)
}

// GetFlexAdvisorResponseErrorsCountForTest returns the current count for a given reason (only for tests).
func GetFlexAdvisorResponseErrorsCountForTest(reason FAResponseErrorReason) (float64, error) {
	gauge := flexAdvisorResponseErrors.WithLabelValues(string(reason))
	return testutil.GetCounterMetricValue(gauge)
}
