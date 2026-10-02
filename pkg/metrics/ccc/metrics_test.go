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

package ccc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

const processStartTimeMetricName = "process_start_time_seconds"

func TestCccMetricName(t *testing.T) {
	testCases := []struct {
		name     string
		baseName string
		expected string
	}{
		{
			name:     "simple base name",
			baseName: "cluster_node_provisioning_attempts_count",
			expected: "cluster_node_provisioning_attempts_count_per_ccc",
		},
		{
			name:     "empty base name",
			baseName: "",
			expected: "_per_ccc",
		},
		{
			name:     "base name with underscores",
			baseName: "some_other_metric",
			expected: "some_other_metric_per_ccc",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, cccMetricName(tc.baseName))
		})
	}
}

// prom-to-sd needs process_start_time_seconds as a single-series gauge in the
// same scrape to set the start time of cumulative metrics (b/566293374).
func TestRegistryExposesProcessStartTime(t *testing.T) {
	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() returned unexpected error: %v", err)
	}

	var family *dto.MetricFamily
	for _, f := range families {
		if f.GetName() == processStartTimeMetricName {
			family = f
			break
		}
	}
	if family == nil {
		t.Fatalf("metric family %q not found in the per-CCC registry", processStartTimeMetricName)
	}
	assert.Equal(t, dto.MetricType_GAUGE, family.GetType())
	if len(family.GetMetric()) != 1 {
		t.Fatalf("metric family %q has %d series, want exactly 1", processStartTimeMetricName, len(family.GetMetric()))
	}

	metric := family.GetMetric()[0]
	assert.Empty(t, metric.GetLabel())
	startTime := metric.GetGauge().GetValue()
	assert.Greater(t, startTime, float64(0))
	assert.LessOrEqual(t, startTime, float64(time.Now().UnixNano())/float64(time.Second))
}

func TestMetricsRegistryHandlerServesProcessStartTime(t *testing.T) {
	recorder := httptest.NewRecorder()
	MetricsRegistryHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics/ccc", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code %d, want %d", recorder.Code, http.StatusOK)
	}
	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	assert.True(t, strings.Contains(string(body), "\n"+processStartTimeMetricName+" "),
		"expected %q in response body:\n%s", processStartTimeMetricName, body)
}
