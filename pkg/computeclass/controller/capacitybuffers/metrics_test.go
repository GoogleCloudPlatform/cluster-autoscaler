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
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGetBufferStatus(t *testing.T) {
	activeStrat := activeCapacityStrategy
	standbyStrat := standbyCapacityStrategy
	customStrat := "custom-strategy"

	tests := []struct {
		description string
		cb          *cbv1beta1.CapacityBuffer
		expectStat  string
	}{
		{
			description: "nil object returns unprocessed",
			cb:          nil,
			expectStat:  bufferStatusUnprocessed,
		},
		{
			description: "custom provisioning strategy returns unprocessed",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &customStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
					},
				},
			},
			expectStat: bufferStatusUnprocessed,
		},
		{
			description: "empty status conditions (fresh/unprocessed buffer) returns unprocessed",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &activeStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{},
				},
			},
			expectStat: bufferStatusUnprocessed,
		},
		{
			description: "ready true without provisioning condition returns accepted",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &activeStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
					},
				},
			},
			expectStat: bufferStatusAccepted,
		},
		{
			description: "ready true and provisioning true returns provisioning",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &activeStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
						{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionTrue, Reason: "FakePodsInjected"},
					},
				},
			},
			expectStat: bufferStatusProvisioning,
		},
		{
			description: "ready true and provisioning false with failure reason returns failed",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &activeStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
						{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionFalse, Reason: "BufferIsEmpty"},
					},
				},
			},
			expectStat: bufferStatusFailed,
		},
		{
			description: "ready false returns failed",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &activeStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionFalse, Reason: "error"},
					},
				},
			},
			expectStat: bufferStatusFailed,
		},
		{
			description: "ready false with stale provisioning true returns failed",
			cb: &cbv1beta1.CapacityBuffer{
				Spec: cbv1beta1.CapacityBufferSpec{
					ProvisioningStrategy: &standbyStrat,
				},
				Status: cbv1beta1.CapacityBufferStatus{
					Conditions: []metav1.Condition{
						{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionFalse, Reason: "error"},
						{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionTrue, Reason: "FakePodsInjected"},
					},
				},
			},
			expectStat: bufferStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := getBufferStatus(tt.cb); got != tt.expectStat {
				t.Fatalf("Expected Buffer status: %s, got %s", tt.expectStat, got)
			}
		})
	}
}

type mockReader struct {
	buffers []cbv1beta1.CapacityBuffer
}

func (m *mockReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return nil
}

func (m *mockReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	cbList, ok := list.(*cbv1beta1.CapacityBufferList)
	if !ok {
		return nil
	}
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	var matched []cbv1beta1.CapacityBuffer
	for _, cb := range m.buffers {
		if listOpts.Namespace != "" && cb.Namespace != listOpts.Namespace {
			continue
		}
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labels.Set(cb.Labels)) {
			continue
		}
		matched = append(matched, cb)
	}
	cbList.Items = matched
	return nil
}

func TestCapacityBuffersCollector(t *testing.T) {
	activeStrat := activeCapacityStrategy
	standbyStrat := standbyCapacityStrategy
	customStrat := "custom-strategy"

	cbActiveAccepted := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-active-accepted",
			Namespace: NamespaceGkeManagedCCC,
			Labels:    map[string]string{labelManagedBy: managedByController},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
			},
		},
	}

	cbStandbyProv := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-standby-prov",
			Namespace: NamespaceGkeManagedCCC,
			Labels:    map[string]string{labelManagedBy: managedByController},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &standbyStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
				{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionTrue, Reason: "FakePodsInjected"},
			},
		},
	}

	// Buffer in wrong namespace should be filtered out
	cbOtherNamespace := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-other-ns",
			Namespace: "default",
			Labels:    map[string]string{labelManagedBy: managedByController},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
	}

	// Buffer without managed-by label should be filtered out
	cbUnmanaged := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-unmanaged",
			Namespace: NamespaceGkeManagedCCC,
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
	}

	// Custom strategy buffer
	cbCustom := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-custom",
			Namespace: NamespaceGkeManagedCCC,
			Labels:    map[string]string{labelManagedBy: managedByController},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &customStrat,
		},
	}

	// Terminating buffer should be excluded from metric counts
	now := metav1.Now()
	cbTerminating := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cb-terminating",
			Namespace:         NamespaceGkeManagedCCC,
			Labels:            map[string]string{labelManagedBy: managedByController},
			DeletionTimestamp: &now,
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue},
			},
		},
	}

	mockClient := &mockReader{
		buffers: []cbv1beta1.CapacityBuffer{cbActiveAccepted, cbStandbyProv, cbOtherNamespace, cbUnmanaged, cbCustom, cbTerminating},
	}

	collector := &capacityBuffersCollector{client: mockClient}

	descChan := make(chan *prometheus.Desc, 1)
	collector.Describe(descChan)
	desc := <-descChan
	if desc == nil {
		t.Fatalf("Expected non-nil descriptor from Describe")
	}

	metricChan := make(chan prometheus.Metric, 20)
	collector.Collect(metricChan)
	close(metricChan)

	metricsMap := make(map[string]float64)
	for m := range metricChan {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatalf("Failed to write metric protobuf: %v", err)
		}
		var strat, stat string
		for _, lp := range pb.Label {
			if lp.GetName() == "provisioning_strategy" {
				strat = lp.GetValue()
			}
			if lp.GetName() == "status" {
				stat = lp.GetValue()
			}
		}
		metricsMap[strat+":"+stat] = pb.GetGauge().GetValue()
	}

	// Check populated metrics
	if val := metricsMap[activeCapacityStrategy+":"+bufferStatusAccepted]; val != 1.0 {
		t.Errorf("Expected active:accepted to be 1.0, got %v", val)
	}
	if val := metricsMap[standbyCapacityStrategy+":"+bufferStatusProvisioning]; val != 1.0 {
		t.Errorf("Expected standby:provisioning to be 1.0, got %v", val)
	}
	if val := metricsMap["custom:"+bufferStatusUnprocessed]; val != 1.0 {
		t.Errorf("Expected custom:unprocessed to be 1.0, got %v", val)
	}

	// Check zero-initialized permutations (e.g. active:failed should be 0.0, not missing)
	if val, exists := metricsMap[activeCapacityStrategy+":"+bufferStatusFailed]; !exists || val != 0.0 {
		t.Errorf("Expected active:failed to be zero-initialized to 0.0, got %v (exists: %v)", val, exists)
	}
	if val, exists := metricsMap[standbyCapacityStrategy+":"+bufferStatusAccepted]; !exists || val != 0.0 {
		t.Errorf("Expected standby:accepted to be zero-initialized to 0.0, got %v (exists: %v)", val, exists)
	}

	// Total permutations: 3 strategies * 4 statuses = 12
	if len(metricsMap) != 12 {
		t.Errorf("Expected 12 total metric permutations emitted, got %d: %v", len(metricsMap), metricsMap)
	}
}

func TestRecordReconcileNoPanic(t *testing.T) {
	recordReconcile("success", 100*time.Millisecond)
	recordReconcile("error", 50*time.Millisecond)
}
