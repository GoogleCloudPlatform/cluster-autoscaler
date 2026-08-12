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

// GetMachineConfigSourceInfoValueForTest returns the current value for a given family and source (only for tests).
func GetMachineConfigSourceInfoValueForTest(machineFamily, configSource string) (float64, error) {
	gauge := machineConfigSourceInfo.WithLabelValues(machineFamily, configSource)
	return testutil.GetGaugeMetricValue(gauge)
}

// GetNodesWithAllocationStrategyCountForTest returns the current count for given labels (only for tests).
func GetNodesWithAllocationStrategyCountForTest(requestedStrategy string, fallbackReason AllocationStrategyFallbackReason, machineType string) (float64, error) {
	counter := nodesWithAllocationStrategy.WithLabelValues(requestedStrategy, string(fallbackReason), machineType)
	return testutil.GetCounterMetricValue(counter)
}

// GetCCStatusUpdatesCountForTest returns the current count for a given status (only for tests).
func GetCCStatusUpdatesCountForTest(status string) (float64, error) {
	counter := ccStatusUpdatesTotal.WithLabelValues(status)
	return testutil.GetCounterMetricValue(counter)
}

// GetCCStatusApiPatchRequestsCountForTest returns the current count for a given code (only for tests).
func GetCCStatusApiPatchRequestsCountForTest(code string) (float64, error) {
	counter := ccStatusApiPatchRequestsTotal.WithLabelValues(code)
	return testutil.GetCounterMetricValue(counter)
}

// GetCCStatusApiPatchDurationCountForTest returns the current sample count for a given code (only for tests).
func GetCCStatusApiPatchDurationCountForTest(code string) (uint64, error) {
	histogram := ccStatusApiPatchDuration.WithLabelValues(code)
	return testutil.GetHistogramMetricCount(histogram)
}

// ResetAllForTest resets all metrics that support it, preventing cross-test
// state contamination. It iterates the same allMetrics slice used by RegisterAll,
// so any newly added metric is automatically handled.
func ResetAllForTest() {
	for _, m := range allMetrics {
		if r, ok := m.(interface{ Reset() }); ok {
			r.Reset()
		}
	}
}

// GetDaemonSetMutationResolutionsCountForTest returns the current count for a given status (only for tests).
func GetDaemonSetMutationResolutionsCountForTest(status string) (float64, error) {
	counter := dsMutationResolutionsTotal.WithLabelValues(status)
	return testutil.GetCounterMetricValue(counter)
}

// GetDaemonSetMutationResolutionDurationSumForTest returns the sum of observed durations (only for tests).
func GetDaemonSetMutationResolutionDurationSumForTest() (float64, error) {
	return testutil.GetHistogramMetricValue(dsMutationResolutionDuration.ObserverMetric)
}
