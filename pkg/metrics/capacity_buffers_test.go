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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

func TestUpdateCapacityBufferResources(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	assertGaugeValue := func(gauge *k8smetrics.GaugeVec, expected float64, labels ...string) {
		t.Helper()
		val, err := testutil.GetGaugeMetricValue(gauge.WithLabelValues(labels...))
		assert.NoError(t, err)
		assert.Equal(t, expected, val)
	}

	cb1ReadyKey := CapacityBufferResourceKey{
		EntityNamespace:      "ns1",
		EntityName:           "buf1",
		EntityUID:            "cb1",
		ProvisioningStrategy: "strategy1",
		State:                CapacityBufferPodStateReady,
	}
	compute := map[CapacityBufferResourceKey]CapacityBufferResources{
		cb1ReadyKey: {
			CpuCores:              2.5,
			MemoryBytes:           1024,
			EphemeralStorageBytes: 2048,
		},
		{ProvisioningStrategy: "strategy2", State: CapacityBufferPodStateProvisioning}: {
			CpuCores:              4.0,
			MemoryBytes:           4096,
			EphemeralStorageBytes: 8192,
		},
	}
	accelerators := map[CapacityBufferAcceleratorKey]int64{
		{CapacityBufferResourceKey: cb1ReadyKey, ResourceName: "nvidia.com/gpu", Model: "nvidia-tesla-t4"}:                                                               4,
		{CapacityBufferResourceKey: cb1ReadyKey, ResourceName: "nvidia.com/gpu", Model: "nvidia-l4"}:                                                                     2,
		{CapacityBufferResourceKey: CapacityBufferResourceKey{ProvisioningStrategy: "strategy2", State: CapacityBufferPodStateNotReady}, ResourceName: "google.com/tpu"}: 8,
	}

	pm.UpdateCapacityBufferResources(compute, accelerators)

	// Verify metrics for ns1/buf1 (cb1), strategy1, ready.
	assertGaugeValue(capacityBufferCpuRequestCoresMetric, 2.5, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	assertGaugeValue(capacityBufferMemoryRequestBytesMetric, 1024, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	assertGaugeValue(capacityBufferEphemeralStorageRequestBytesMetric, 2048, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	// Accelerators of different models are reported as separate series.
	assertGaugeValue(capacityBufferAcceleratorRequestDevicesMetric, 4, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready", "nvidia.com/gpu", "nvidia-tesla-t4")
	assertGaugeValue(capacityBufferAcceleratorRequestDevicesMetric, 2, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready", "nvidia.com/gpu", "nvidia-l4")

	// Verify metrics for aggregated ("", "", ""), strategy2.
	assertGaugeValue(capacityBufferCpuRequestCoresMetric, 4.0, CapacityBufferEntityType, "", "", "", "strategy2", "provisioning")
	assertGaugeValue(capacityBufferMemoryRequestBytesMetric, 4096, CapacityBufferEntityType, "", "", "", "strategy2", "provisioning")
	assertGaugeValue(capacityBufferEphemeralStorageRequestBytesMetric, 8192, CapacityBufferEntityType, "", "", "", "strategy2", "provisioning")
	assertGaugeValue(capacityBufferAcceleratorRequestDevicesMetric, 8, CapacityBufferEntityType, "", "", "", "strategy2", "unready", "google.com/tpu", "")

	// Calling UpdateCapacityBufferResources with new maps resets previous series.
	compute2 := map[CapacityBufferResourceKey]CapacityBufferResources{
		{EntityNamespace: "ns3", EntityName: "buf3", EntityUID: "cb3", ProvisioningStrategy: "strategy3", State: CapacityBufferPodStateNotReady}: {
			CpuCores:              1.0,
			MemoryBytes:           512,
			EphemeralStorageBytes: 256,
		},
	}
	accelerators2 := map[CapacityBufferAcceleratorKey]int64{
		{
			CapacityBufferResourceKey: CapacityBufferResourceKey{EntityNamespace: "ns3", EntityName: "buf3", EntityUID: "cb3", ProvisioningStrategy: "strategy3", State: CapacityBufferPodStateProvisioning},
			ResourceName:              "nvidia.com/gpu",
			Model:                     "nvidia-tesla-a100",
		}: 2,
	}
	pm.UpdateCapacityBufferResources(compute2, accelerators2)

	assertGaugeValue(capacityBufferCpuRequestCoresMetric, 0, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	assertGaugeValue(capacityBufferMemoryRequestBytesMetric, 0, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	assertGaugeValue(capacityBufferEphemeralStorageRequestBytesMetric, 0, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready")
	assertGaugeValue(capacityBufferAcceleratorRequestDevicesMetric, 0, CapacityBufferEntityType, "ns1", "buf1", "cb1", "strategy1", "ready", "nvidia.com/gpu", "nvidia-tesla-t4")
	assertGaugeValue(capacityBufferCpuRequestCoresMetric, 1.0, CapacityBufferEntityType, "ns3", "buf3", "cb3", "strategy3", "unready")
	assertGaugeValue(capacityBufferAcceleratorRequestDevicesMetric, 2, CapacityBufferEntityType, "ns3", "buf3", "cb3", "strategy3", "provisioning", "nvidia.com/gpu", "nvidia-tesla-a100")
}

func TestCapacityBuffersMetricsRegistryHandler(t *testing.T) {
	registerOnce.Do(RegisterAll)
	ResetAllForTest()
	pm := &prometheusMetrics{}

	key := CapacityBufferResourceKey{
		EntityNamespace:      "ns1",
		EntityName:           "buf1",
		EntityUID:            "cb1",
		ProvisioningStrategy: "strategy1",
		State:                CapacityBufferPodStateReady,
	}
	pm.UpdateCapacityBufferResources(
		map[CapacityBufferResourceKey]CapacityBufferResources{
			key: {CpuCores: 2.5, MemoryBytes: 1024, EphemeralStorageBytes: 2048},
		},
		map[CapacityBufferAcceleratorKey]int64{
			{CapacityBufferResourceKey: key, ResourceName: "nvidia.com/gpu", Model: "nvidia-tesla-t4"}: 4,
		},
	)

	recorder := httptest.NewRecorder()
	CapacityBuffersMetricsRegistryHandler(true).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics/capacitybuffers", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)

	bodyBytes, err := io.ReadAll(recorder.Body)
	assert.NoError(t, err)
	body := string(bodyBytes)

	expectedMetrics := []string{
		"process_start_time_seconds",
		"cluster_autoscaler_capacity_buffer_cpu_request_cores",
		"cluster_autoscaler_capacity_buffer_memory_request_bytes",
		"cluster_autoscaler_capacity_buffer_ephemeral_storage_request_bytes",
		"cluster_autoscaler_capacity_buffer_accelerator_request_devices",
	}
	for _, metricName := range expectedMetrics {
		assert.True(t, strings.Contains(body, metricName), "expected %q in /metrics/capacitybuffers response body", metricName)
	}

	// Verify that when disabled, /metrics/capacitybuffers still returns 200 OK with an empty body.
	disabledRecorder := httptest.NewRecorder()
	CapacityBuffersMetricsRegistryHandler(false).ServeHTTP(disabledRecorder, httptest.NewRequest(http.MethodGet, "/metrics/capacitybuffers", nil))
	assert.Equal(t, http.StatusOK, disabledRecorder.Code)
	disabledBodyBytes, err := io.ReadAll(disabledRecorder.Body)
	assert.NoError(t, err)
	assert.Empty(t, string(disabledBodyBytes))

	// Verify that capacity buffer resource request metrics are not exposed on the default legacyregistry.
	defaultFamilies, err := legacyregistry.DefaultGatherer.Gather()
	assert.NoError(t, err)
	for _, f := range defaultFamilies {
		for _, metricName := range expectedMetrics[1:] {
			assert.NotEqual(t, metricName, f.GetName(), "metric %q should not be registered in legacyregistry", metricName)
		}
	}
}

func TestToCapacityBufferResourceState(t *testing.T) {
	testCases := []struct {
		podState CapacityBufferPodState
		expected capacityBufferResourceState
	}{
		{
			podState: CapacityBufferPodStateReady,
			expected: capacityBufferResourceStateReady,
		},
		{
			podState: CapacityBufferPodStateProvisioning,
			expected: capacityBufferResourceStateProvisioning,
		},
		{
			podState: CapacityBufferPodStateNotReady,
			expected: capacityBufferResourceStateUnready,
		},
		{
			podState: CapacityBufferPodState("unknown"),
			expected: capacityBufferResourceStateUnready,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.podState), func(t *testing.T) {
			assert.Equal(t, tc.expected, toCapacityBufferResourceState(tc.podState))
		})
	}
}
