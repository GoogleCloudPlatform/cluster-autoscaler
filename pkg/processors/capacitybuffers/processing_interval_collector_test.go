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

package capacitybuffers

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	cbmetrics "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/metrics"
)

type mockCapacityBufferClient struct {
	buffers []*v1beta1.CapacityBuffer
	err     error
}

func (m *mockCapacityBufferClient) ListCapacityBuffers(namespace string) ([]*v1beta1.CapacityBuffer, error) {
	return m.buffers, m.err
}

func collectMetrics(t *testing.T, collector prometheus.Collector) map[string]float64 {
	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	results := make(map[string]float64)
	for m := range ch {
		dtoMetric := &dto.Metric{}
		err := m.Write(dtoMetric)
		assert.NoError(t, err)

		var isNew string
		for _, l := range dtoMetric.GetLabel() {
			if l.GetName() == "is_new_buffer" {
				isNew = l.GetValue() // "true" or "false"
			}
		}
		results[isNew] = dtoMetric.GetGauge().GetValue() // the actual value of the metric we got
	}
	return results
}

func TestProcessingIntervalCollector_Empty(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)
	client := &mockCapacityBufferClient{buffers: nil}
	cache := cbmetrics.NewReconciliationCache()
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	assert.Equal(t, float64(0), results["true"])
	assert.Equal(t, float64(0), results["false"])
}

func TestProcessingIntervalCollector_ExistingBufferInCache(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)

	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cb1",
			Namespace:         "default",
			UID:               types.UID("uid-1"),
			CreationTimestamp: metav1.NewTime(now.Add(-600 * time.Second)),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue},
			},
		},
	}

	client := &mockCapacityBufferClient{buffers: []*v1beta1.CapacityBuffer{cb}}
	cache := cbmetrics.NewReconciliationCache()
	// Reconciled 120 seconds ago
	cache.Update([]*v1beta1.CapacityBuffer{cb}, now.Add(-120*time.Second))
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	assert.Equal(t, float64(0), results["true"])
	assert.Equal(t, float64(120), results["false"])
}

func TestProcessingIntervalCollector_NewBuffer(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)

	// Created 45 seconds ago, not in cache, no conditions
	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cb-new",
			Namespace:         "default",
			UID:               types.UID("uid-new"),
			CreationTimestamp: metav1.NewTime(now.Add(-45 * time.Second)),
		},
	}

	client := &mockCapacityBufferClient{buffers: []*v1beta1.CapacityBuffer{cb}}
	cache := cbmetrics.NewReconciliationCache()
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	assert.Equal(t, float64(45), results["true"])
	assert.Equal(t, float64(0), results["false"])
}

func TestProcessingIntervalCollector_MultipleBuffersMaxDelay(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)

	existing1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing1",
			Namespace: "default",
			UID:       types.UID("uid-e1"),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue}},
		},
	}
	existing2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing2-csn",
			Namespace: "default",
			UID:       types.UID("uid-e2"),
		},
		Spec: v1beta1.CapacityBufferSpec{
			ProvisioningStrategy: ptr.To(ColdProvisioningStrategy),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue}},
		},
	}

	// status is empty for new buffers
	new1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "new1-asn",
			Namespace:         "default",
			UID:               types.UID("uid-n1"),
			CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Second)),
		},
	}
	new2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "new2-csn",
			Namespace:         "default",
			UID:               types.UID("uid-n2"),
			CreationTimestamp: metav1.NewTime(now.Add(-50 * time.Second)),
		},
		Spec: v1beta1.CapacityBufferSpec{
			ProvisioningStrategy: ptr.To(ColdProvisioningStrategy),
		},
	}

	client := &mockCapacityBufferClient{
		buffers: []*v1beta1.CapacityBuffer{existing1, existing2, new1, new2},
	}
	cache := cbmetrics.NewReconciliationCache()
	cache.Update([]*v1beta1.CapacityBuffer{existing1}, now.Add(-100*time.Second))
	cache.Update([]*v1beta1.CapacityBuffer{existing2}, now.Add(-300*time.Second))
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	assert.Equal(t, float64(50), results["true"])   // max(20, 50)
	assert.Equal(t, float64(300), results["false"]) // max(100, 300)
}

func TestProcessingIntervalCollector_PostRestartSkip(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)

	// Buffer has conditions but is NOT in cache (CA restarted)
	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cb-restart",
			Namespace:         "default",
			UID:               types.UID("uid-restart"),
			CreationTimestamp: metav1.NewTime(now.Add(-3600 * time.Second)),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue},
			},
		},
	}

	client := &mockCapacityBufferClient{buffers: []*v1beta1.CapacityBuffer{cb}}
	cache := cbmetrics.NewReconciliationCache() // empty cache
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	// Post-restart buffer should be skipped, reporting 0
	assert.Equal(t, float64(0), results["true"])
	assert.Equal(t, float64(0), results["false"])
}

func TestProcessingIntervalCollector_UnsupportedStrategy(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(now)

	supportedCB := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "supported-cb",
			Namespace:         "default",
			UID:               types.UID("uid-supported"),
			CreationTimestamp: metav1.NewTime(now.Add(-300 * time.Second)),
		},
		Spec: v1beta1.CapacityBufferSpec{
			ProvisioningStrategy: ptr.To(ColdProvisioningStrategy),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue},
			},
		},
	}
	unsupportedCB := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "unsupported-cb",
			Namespace:         "default",
			UID:               types.UID("uid-unsupported"),
			CreationTimestamp: metav1.NewTime(now.Add(-600 * time.Second)),
		},
		Spec: v1beta1.CapacityBufferSpec{
			ProvisioningStrategy: ptr.To("unsupported-strategy"),
		},
		Status: v1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue},
			},
		},
	}

	client := &mockCapacityBufferClient{
		buffers: []*v1beta1.CapacityBuffer{supportedCB, unsupportedCB},
	}
	cache := cbmetrics.NewReconciliationCache()
	cache.Update([]*v1beta1.CapacityBuffer{supportedCB}, now.Add(-60*time.Second))
	cache.Update([]*v1beta1.CapacityBuffer{unsupportedCB}, now.Add(-300*time.Second)) // unsported has larger reconciled time, but is skipped as unsupported

	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy}

	collector := NewProcessingIntervalCollector(client, strategies, cache, fakeClock)
	results := collectMetrics(t, collector)

	// Unsupported buffer is excluded, so only supportedCB (60s) is reported
	assert.Equal(t, float64(0), results["true"])
	assert.Equal(t, float64(60), results["false"])
}
