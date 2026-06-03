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

package processors

import (
	"testing"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

func TestScenario_HappyPath_SpecLevel(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	// Step 1: Autoscaler decides to scale up
	t1 := t0.Add(time.Minute)
	observer.OnScaleUpDecision("cc1", -1, t1)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesReactionLatency", false, time.Minute)

	// Step 2: Provisioning completes successfully
	t2 := t0.Add(2 * time.Minute)
	observer.OnProvisioningComplete("cc1", -1, t2)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, false, 2*time.Minute)
}

func TestScenario_HappyPath_PriorityLevel(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{
				{
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
				},
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	// Step 1: Autoscaler decides to scale up
	t1 := t0.Add(time.Minute)
	observer.OnScaleUpDecision("cc1", 0, t1)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesReactionLatency", true, time.Minute)

	// Step 2: Provisioning completes successfully
	t2 := t0.Add(2 * time.Minute)
	observer.OnProvisioningComplete("cc1", 0, t2)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, true, 2*time.Minute)
}

func TestScenario_ProvisioningErrorAndRecovery(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	// Step 1: Provisioning encounters an error (e.g. QuotaExceeded)
	t1 := t0.Add(time.Minute)
	observer.OnProvisioningError("cc1", "quota_exceeded", true, t1)

	// Step 2: Remains unprovisioned for > 30 mins
	tLong := t0.Add(31 * time.Minute)
	observer.CheckLongUnprovisioned(tLong)
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample{
		{
			ProvisioningErrorEncountered: "quota_exceeded",
			Unhelpable:                   true,
			DefinedInPriority:            false,
		},
	})

	// Step 3: Quota increased, provisioning completes successfully
	tComplete := t0.Add(35 * time.Minute)
	observer.OnProvisioningComplete("cc1", -1, tComplete)
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample(nil))
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "quota_exceeded", true, false, 35*time.Minute)
}

func TestScenario_LogicalUpdateResetsTimers(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	// Step 1: Update logically unchanged, should NOT reset timer
	cc1Updated := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}
	t1 := t0.Add(10 * time.Minute)
	observer.OnComputeClassUpdated(cc1, cc1Updated, t1)

	// Step 2: Update with logical change (TargetNodeCount changed), should reset timer
	cc1LogicalUpdate := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(10),
			},
		},
	}
	t2 := t0.Add(20 * time.Minute)
	observer.OnComputeClassUpdated(cc1Updated, cc1LogicalUpdate, t2)

	// Step 3: Provisioning completes 5 mins after logical update. Latency should be 5 mins (t3 - t2), not 25 mins (t3 - t0).
	t3 := t2.Add(5 * time.Minute)
	observer.OnProvisioningComplete("cc1", -1, t3)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, false, 5*time.Minute)
}

func TestScenario_DeletionCleansUpMetrics(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	observer.OnProvisioningError("cc1", "stockout", true, t0.Add(time.Minute))
	observer.CheckLongUnprovisioned(t0.Add(31 * time.Minute))
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample{
		{
			ProvisioningErrorEncountered: "stockout",
			Unhelpable:                   true,
			DefinedInPriority:            false,
		},
	})

	observer.OnComputeClassDeleted("cc1")
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample(nil))
}

func TestScenario_ShortfallDetected_GatingAndRecovery(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := NewMinCapacityObserver(metrics, nil)

	t0 := time.Now()
	observer.OnComputeClassAdded(cc1, t0)

	// Step 1: Shortfall detected during initial boot (provisioningLatencyEmitted == false).
	// Should NOT reset timer.
	t1 := t0.Add(time.Minute)
	observer.OnShortfallDetected("cc1", -1, t1)

	concreteObserver := observer.(*minCapacityObserver)
	assert.Equal(t, t0, concreteObserver.ccStates["cc1"].spec.firstObservedAt)

	// Step 2: Initial provisioning completes successfully.
	t2 := t0.Add(2 * time.Minute)
	observer.OnProvisioningComplete("cc1", -1, t2)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, false, 2*time.Minute)

	// Step 3: Node dies at t3 (regression). Shortfall detected.
	// Because provisioningLatencyEmitted == true, it SHOULD reset timer to t3.
	t3 := t0.Add(5 * time.Minute)
	observer.OnShortfallDetected("cc1", -1, t3)
	assert.Equal(t, t3, concreteObserver.ccStates["cc1"].spec.firstObservedAt)
	assert.False(t, concreteObserver.ccStates["cc1"].spec.provisioningLatencyEmitted)

	// Step 4: Replacement node finishes provisioning 1 minute later (t4).
	t4 := t3.Add(time.Minute)
	observer.OnProvisioningComplete("cc1", -1, t4)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, false, time.Minute)
}

func TestOnProvisioningError(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := &minCapacityObserver{
		metrics:  metrics,
		ccStates: make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	state := observer.ccStates["cc1"]
	assert.Equal(t, "", state.spec.lastError)
	assert.False(t, state.spec.unhelpable)

	observer.OnProvisioningError("cc1", "quota_exceeded", true, now.Add(time.Minute))

	assert.Equal(t, "quota_exceeded", state.spec.lastError)
	assert.True(t, state.spec.unhelpable)
}

func TestOnProvisioningComplete_WithFallback(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := &minCapacityObserver{
		metrics:  metrics,
		ccStates: make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	state := observer.ccStates["cc1"]
	assert.False(t, state.spec.reactionLatencyEmitted)
	assert.False(t, state.spec.provisioningLatencyEmitted)

	observer.OnProvisioningComplete("cc1", -1, now.Add(time.Minute))

	assert.True(t, state.spec.reactionLatencyEmitted)
	assert.True(t, state.spec.provisioningLatencyEmitted)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesReactionLatency", false, time.Minute)
	metrics.AssertCalled(t, "ObserveCcMinTargetNodesProvisioningLatency", "", false, false, time.Minute)
}

func TestCheckLongUnprovisioned_GaugeReset(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := &minCapacityObserver{
		metrics:  metrics,
		ccStates: make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	observer.CheckLongUnprovisioned(now.Add(31 * time.Minute))
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample{
		{
			ProvisioningErrorEncountered: "",
			Unhelpable:                   false,
			DefinedInPriority:            false,
		},
	})

	observer.OnProvisioningComplete("cc1", -1, now.Add(32*time.Minute))
	metrics.AssertCalled(t, "ObserveCcLongUnprovisionedMinTargetNodesCount", []internalmetrics.CcLongUnprovisionedSample(nil))
}

func TestPriorityLevelTracking(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{
				{
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
				},
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := &minCapacityObserver{
		metrics:  metrics,
		ccStates: make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	// Verify state created for priority 0
	state := observer.ccStates["cc1"]
	assert.Contains(t, state.priorities, 0)
	assert.Equal(t, now, state.priorities[0].firstObservedAt)

	// Check update with logically unchanged Priority
	cc1Updated := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{
				{
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
				},
			},
		},
	}
	later := now.Add(time.Minute)
	observer.OnComputeClassUpdated(cc1, cc1Updated, later)
	assert.Equal(t, now, state.priorities[0].firstObservedAt)

	// Update with Priority MinimumCapacity TargetNodeCount change
	cc1PriorityTargetChange := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{
				{
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(10),
					},
				},
			},
		},
	}
	evenLater := later.Add(time.Minute)
	observer.OnComputeClassUpdated(cc1Updated, cc1PriorityTargetChange, evenLater)
	assert.Equal(t, evenLater, state.priorities[0].firstObservedAt)
}

func TestPriorityLevelTracking_SpecReset(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(2),
			},
			Priorities: []cccv1.Priority{
				{
					MachineType: stringPtr("e2-standard-4"),
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
				},
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	observer := &minCapacityObserver{
		metrics:  metrics,
		ccStates: make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	state := observer.ccStates["cc1"]
	assert.Equal(t, now, state.spec.firstObservedAt)
	assert.Equal(t, now, state.priorities[0].firstObservedAt)

	// 1. Change only priority-level targetNodeCount
	cc1TargetChange := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(2),
			},
			Priorities: []cccv1.Priority{
				{
					MachineType: stringPtr("e2-standard-4"),
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(10),
					},
				},
			},
		},
	}
	later := now.Add(time.Minute)
	observer.OnComputeClassUpdated(cc1, cc1TargetChange, later)

	// Spec level timestamp should NOT be reset
	assert.Equal(t, now, state.spec.firstObservedAt)
	// Priority level timestamp should be reset
	assert.Equal(t, later, state.priorities[0].firstObservedAt)

	// 2. Change priority-level other field (e.g. machine type)
	cc1MachineTypeChange := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(2),
			},
			Priorities: []cccv1.Priority{
				{
					MachineType: stringPtr("n2-standard-4"),
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(10),
					},
				},
			},
		},
	}
	evenLater := later.Add(time.Minute)
	observer.OnComputeClassUpdated(cc1TargetChange, cc1MachineTypeChange, evenLater)

	// Spec level timestamp SHOULD be reset
	assert.Equal(t, evenLater, state.spec.firstObservedAt)
	// Priority level timestamp SHOULD be reset
	assert.Equal(t, evenLater, state.priorities[0].firstObservedAt)
}

func stringPtr(s string) *string {
	return &s
}

type mockCRDStatus struct {
	crd.CRDStatus
	conditions     []metav1.Condition
	ruleConditions map[string][]metav1.Condition
}

func (m *mockCRDStatus) UpdateConditions(conds []metav1.Condition) {
	m.conditions = conds
}

func (m *mockCRDStatus) GetConditions() []metav1.Condition {
	return m.conditions
}

func (m *mockCRDStatus) UpdateRuleConditions(ruleIdx string, conds []metav1.Condition) {
	if m.ruleConditions == nil {
		m.ruleConditions = make(map[string][]metav1.Condition)
	}
	m.ruleConditions[ruleIdx] = conds
}

func (m *mockCRDStatus) GetRuleConditions(ruleIdx string) []metav1.Condition {
	if m.ruleConditions == nil {
		return nil
	}
	return m.ruleConditions[ruleIdx]
}

func TestOnScaleUpDecision_Conditions(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	updatesCh := make(chan status.UpdateMessage, 10)
	observer := &minCapacityObserver{
		metrics:   metrics,
		updatesCh: updatesCh,
		ccStates:  make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	observer.OnScaleUpDecision("cc1", -1, now.Add(time.Minute))

	assert.Equal(t, 1, len(updatesCh))
	msg := <-updatesCh
	assert.Equal(t, "cc1", msg.Id.CRDName)
	assert.Equal(t, gkelabels.ComputeClassLabel, msg.Id.CRDLabel)

	mockStatus := &mockCRDStatus{}
	msg.Mutate(mockStatus)

	assert.Len(t, mockStatus.conditions, 2)
	assert.Equal(t, status.MinCapacityProvisioning, mockStatus.conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, mockStatus.conditions[0].Status)
	assert.Equal(t, status.MinCapacityProvisioned, mockStatus.conditions[1].Type)
	assert.Equal(t, metav1.ConditionFalse, mockStatus.conditions[1].Status)
}

func TestOnProvisioningComplete_Conditions(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	updatesCh := make(chan status.UpdateMessage, 10)
	observer := &minCapacityObserver{
		metrics:   metrics,
		updatesCh: updatesCh,
		ccStates:  make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	observer.OnProvisioningComplete("cc1", -1, now.Add(time.Minute))

	assert.Equal(t, 1, len(updatesCh))
	msg := <-updatesCh

	mockStatus := &mockCRDStatus{
		conditions: []metav1.Condition{
			{Type: status.MinCapacityProvisioning, Status: metav1.ConditionTrue},
		},
	}
	msg.Mutate(mockStatus)

	assert.Len(t, mockStatus.conditions, 1)
	assert.Equal(t, status.MinCapacityProvisioned, mockStatus.conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, mockStatus.conditions[0].Status)
}

func TestOnProvisioningError_Conditions(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			MinimumCapacity: &cccv1.MinimumCapacity{
				TargetNodeCount: intPtr(5),
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	updatesCh := make(chan status.UpdateMessage, 10)
	observer := &minCapacityObserver{
		metrics:   metrics,
		updatesCh: updatesCh,
		ccStates:  make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	observer.OnProvisioningError("cc1", "quota_exceeded", true, now.Add(time.Minute))

	assert.Equal(t, 1, len(updatesCh))
	msg := <-updatesCh

	mockStatus := &mockCRDStatus{}
	msg.Mutate(mockStatus)

	assert.Len(t, mockStatus.conditions, 2)
	assert.Equal(t, status.MinCapacityProvisioning, mockStatus.conditions[0].Type)
	assert.Equal(t, metav1.ConditionFalse, mockStatus.conditions[0].Status)
	assert.Equal(t, status.MinCapacityProvisioned, mockStatus.conditions[1].Type)
	assert.Equal(t, metav1.ConditionFalse, mockStatus.conditions[1].Status)
	assert.Equal(t, status.ProvisioningFailed, mockStatus.conditions[1].Reason)
}

func TestOnScaleUpDecision_PriorityConditions(t *testing.T) {
	cc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc1"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{
				{
					MinimumCapacity: &cccv1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
				},
			},
		},
	}

	metrics := computeclass.NewMockMetrics()
	updatesCh := make(chan status.UpdateMessage, 10)
	observer := &minCapacityObserver{
		metrics:   metrics,
		updatesCh: updatesCh,
		ccStates:  make(map[string]*minCapacityProvisioningState),
	}

	now := time.Now()
	observer.OnComputeClassAdded(cc1, now)

	observer.OnScaleUpDecision("cc1", 0, now.Add(time.Minute))

	assert.Equal(t, 1, len(updatesCh))
	msg := <-updatesCh
	assert.Equal(t, "cc1", msg.Id.CRDName)

	mockStatus := &mockCRDStatus{}
	msg.Mutate(mockStatus)

	ruleIdxStr := "0"
	assert.Contains(t, mockStatus.ruleConditions, ruleIdxStr)
	conds := mockStatus.ruleConditions[ruleIdxStr]
	assert.Len(t, conds, 2)
	assert.Equal(t, status.MinCapacityProvisioning, conds[0].Type)
	assert.Equal(t, metav1.ConditionTrue, conds[0].Status)
}
