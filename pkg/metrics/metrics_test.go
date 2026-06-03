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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

var registerOnce sync.Once

func TestGetRoundedLANodesShapeMap(t *testing.T) {
	tests := []struct {
		name            string
		laPodsNodeShape []LAPodNodeShape
		want            map[roundedLAPodNodeShape]int64
	}{
		{
			name:            "empty input",
			laPodsNodeShape: []LAPodNodeShape{},
			want:            map[roundedLAPodNodeShape]int64{},
		},
		{
			name: "single LA pod, no rounding needed for cpu and memory",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 2000, KBytes: 8 * size.GiBToKiB}, UserWorkloadPodsCount: 4},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 2, memoryGiB: 8, sqrtUserPodsCount: 2}: 1,
			},
		},
		{
			name: "single LA pod, cpu needs rounding up",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1500, KBytes: 8 * size.GiBToKiB}, UserWorkloadPodsCount: 9},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 2, memoryGiB: 8, sqrtUserPodsCount: 3}: 1,
			},
		},
		{
			name: "single LA pod, memory needs rounding up",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 2000, KBytes: 7 * size.GiBToKiB}, UserWorkloadPodsCount: 16},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 2, memoryGiB: 8, sqrtUserPodsCount: 4}: 1,
			},
		},
		{
			name: "single LA node, both cpu and memory need rounding up",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 2001, KBytes: 8*size.GiBToKiB + 1}, UserWorkloadPodsCount: 25},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 4, memoryGiB: 16, sqrtUserPodsCount: 5}: 1,
			},
		},
		{
			name: "multiple LA pods, all round to the same shape",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1000, KBytes: 1 * size.GiBToKiB}, UserWorkloadPodsCount: 1},
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1999, KBytes: 7 * size.GiBToKiB}, UserWorkloadPodsCount: 3},
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1, KBytes: 1}, UserWorkloadPodsCount: 1},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 2, memoryGiB: 8, sqrtUserPodsCount: 1}: 3,
			},
		},
		{
			name: "multiple LA pods, round to different shapes",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1500, KBytes: 7 * size.GiBToKiB}, UserWorkloadPodsCount: 4},  // rounds to 2cpu, 8GiB, 2 sqrt_pods
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 3500, KBytes: 15 * size.GiBToKiB}, UserWorkloadPodsCount: 9}, // rounds to 4cpu, 16GiB, 3 sqrt_pods
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 1800, KBytes: 6 * size.GiBToKiB}, UserWorkloadPodsCount: 4},  // rounds to 2cpu, 8GiB, 2 sqrt_pods (same as first)
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 2, memoryGiB: 8, sqrtUserPodsCount: 2}:  2,
				{cpu: 4, memoryGiB: 16, sqrtUserPodsCount: 3}: 1,
			},
		},
		{
			name: "LA pod with zero cpu, memory, and pods",
			laPodsNodeShape: []LAPodNodeShape{
				{NodeSizeAllocatable: size.Allocatable{MilliCpus: 0, KBytes: 0}, UserWorkloadPodsCount: 0},
			},
			want: map[roundedLAPodNodeShape]int64{
				{cpu: 0, memoryGiB: 0, sqrtUserPodsCount: 0}: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getRoundedLANodesShapeMap(tt.laPodsNodeShape)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUpdateResizeBackoffStatus(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()
	e4aName := machinetypes.E4A.Name()

	// Test Case 1: EK family
	// Expectation: Both resizeBackoffStatus for EK and ekBackoffStatus should be set to 1 (true)
	pm.UpdateResizeBackoffStatus(ekName, true)

	// Verify ekBackoffStatus
	val, err := testutil.GetGaugeMetricValue(ekBackoffStatus)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	// Verify resizeBackoffStatus for EK
	gauge := resizeBackoffStatus.WithLabelValues(ekName)
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	// Test Case 2: E4A family
	// Expectation: resizeBackoffStatus for E4A should be set to 1 (true)
	pm.UpdateResizeBackoffStatus(e4aName, true)

	// Verify resizeBackoffStatus for E4A
	gauge = resizeBackoffStatus.WithLabelValues(e4aName)
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	// Test Case 3: Set backoff to false for EK
	pm.UpdateResizeBackoffStatus(ekName, false)
	val, err = testutil.GetGaugeMetricValue(ekBackoffStatus)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), val)

	gauge = resizeBackoffStatus.WithLabelValues(ekName)
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), val)
}

func TestUpdateResizableVmLaunchStatusNotCreated(t *testing.T) {
	pm := &prometheusMetrics{}

	// Save the original metric to restore it after the test
	originalMetric := resizableVmLaunchStatus
	defer func() { resizableVmLaunchStatus = originalMetric }()

	// Override with an uncreated metric
	resizableVmLaunchStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_launch_status",
			Help:      "Information about resizable vm launch status.",
		},
		[]string{"machine_family", "launch_phase", "launched_from"},
	)

	// Verify the metric is indeed not created
	assert.False(t, resizableVmLaunchStatus.IsCreated())

	// Verify that it doesn't panic when the metric is not created.
	assert.NotPanics(t, func() {
		pm.UpdateResizableVmLaunchStatus("test-family", "phase", "source")
	})
}

func TestUpdateCapacityBufferPods(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	counts := map[CapacityBufferPodsKey]int{
		{ProvisioningStrategy: "strategy1", State: CapacityBufferPodStateReady}:        10,
		{ProvisioningStrategy: "strategy1", State: CapacityBufferPodStateProvisioning}: 5,
		{ProvisioningStrategy: "strategy2", State: CapacityBufferPodStateReady}:        3,
	}

	pm.UpdateCapacityBufferPods(counts)

	// Verify metrics for strategy1, Ready
	gauge := capacityBuffersPodsMetric.WithLabelValues("strategy1", "Ready")
	val, err := testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(10), val)

	// Verify metrics for strategy1, Provisioning
	gauge = capacityBuffersPodsMetric.WithLabelValues("strategy1", "Provisioning")
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), val)

	// Verify metrics for strategy2, Ready
	gauge = capacityBuffersPodsMetric.WithLabelValues("strategy2", "Ready")
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(3), val)

	// Test Reset(): calling UpdateCapacityBufferPods with new map should clear old ones.
	counts2 := map[CapacityBufferPodsKey]int{
		{ProvisioningStrategy: "strategy3", State: CapacityBufferPodStateReady}: 7,
	}
	pm.UpdateCapacityBufferPods(counts2)

	// strategy1 should be gone (or return 0)
	gauge = capacityBuffersPodsMetric.WithLabelValues("strategy1", "Ready")
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), val)

	// strategy3 should be present
	gauge = capacityBuffersPodsMetric.WithLabelValues("strategy3", "Ready")
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(7), val)
}

func TestUpdateCSNEnabled(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	pm.UpdateCSNEnabled(true)
	val, err := testutil.GetGaugeMetricValue(csnEnabled)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	pm.UpdateCSNEnabled(false)
	val, err = testutil.GetGaugeMetricValue(csnEnabled)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), val)
}

func TestSetCSNInvalidCondition(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	pm.SetCSNInvalidCondition(SuspendedNodeWithBlockingPods)
	gauge := csnInvalidCondition.WithLabelValues(string(SuspendedNodeWithBlockingPods))
	val, err := testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	testConditionForTesting := CSNInvalidCondition("TestConditionForTesting")
	pm.SetCSNInvalidCondition(testConditionForTesting)
	gauge = csnInvalidCondition.WithLabelValues(string(testConditionForTesting))
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	// Verify both are present.
	val, err = testutil.GetGaugeMetricValue(csnInvalidCondition.WithLabelValues(string(SuspendedNodeWithBlockingPods)))
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)
	val, err = testutil.GetGaugeMetricValue(csnInvalidCondition.WithLabelValues(string(testConditionForTesting)))
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)
}

func TestRegisterPodsSchedulableOnVmUpsizes(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()
	e4aName := machinetypes.E4A.Name()

	pm.RegisterResizableVmPodsSchedulableOnUpsizes(ekName, 5)

	val, err := testutil.GetCounterMetricValue(podsSchedulableOnEkUpsizes)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), val)

	counter := resizableVmPodsSchedulableOnUpsizes.WithLabelValues(ekName)
	val, err = testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), val)

	pm.RegisterResizableVmPodsSchedulableOnUpsizes(e4aName, 3)

	counter = resizableVmPodsSchedulableOnUpsizes.WithLabelValues(e4aName)
	val, err = testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(3), val)
}

func TestRegisterFixerEvent(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()

	pm.RegisterResizableVmFixerEvents(ekName, "type1", "status1", "source1")

	counter := resizableVmFixerEvents.WithLabelValues(ekName, "type1", "status1", "source1")
	val, err := testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	counter = fixerEvents.WithLabelValues("type1", "status1", "source1")
	val, err = testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)
}

func TestRegisterReconcileNodeStateEvent(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()

	pm.RegisterResizableVmReconcileNodeStateEvents(ekName, 1, "status1", true)

	counter := resizableVmReconcileNodeStateEvents.WithLabelValues(ekName, "1", "status1", "true")
	val, err := testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)

	counter = reconcileNodeStateEvents.WithLabelValues("1", "status1", "true")
	val, err = testutil.GetCounterMetricValue(counter)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)
}

func TestUpdateTotalVmNodesLookaheadSpace(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()
	val := size.Allocatable{MilliCpus: 1000, KBytes: 2000}

	pm.UpdateResizableVmTotalNodesLookaheadSpace(ekName, val)

	g := resizableVmTotalNodesLookaheadSpaceCPU.WithLabelValues(ekName)
	v, err := testutil.GetGaugeMetricValue(g)
	assert.NoError(t, err)
	assert.Equal(t, float64(1000), v)

	g = resizableVmTotalNodesLookaheadSpaceMemory.WithLabelValues(ekName)
	v, err = testutil.GetGaugeMetricValue(g)
	assert.NoError(t, err)
	assert.Equal(t, float64(2000), v)

	v, err = testutil.GetGaugeMetricValue(totalEkNodesLookaheadSpaceCPU)
	assert.NoError(t, err)
	assert.Equal(t, float64(1000), v)

	v, err = testutil.GetGaugeMetricValue(totalEkNodesLookaheadSpaceMemory)
	assert.NoError(t, err)
	assert.Equal(t, float64(2000), v)
}

func TestUpdateUnscheduleableLookaheadPodsCount(t *testing.T) {
	registerOnce.Do(RegisterAll)
	pm := &prometheusMetrics{}

	ekName := machinetypes.EK.Name()
	e4aName := machinetypes.E4A.Name()

	pm.UpdateResizableVmUnschedulableLookaheadPodsCount(ekName, 5)

	val, err := testutil.GetGaugeMetricValue(unscheduleableLookaheadPodsCount)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), val)

	gauge := resizableVmUnschedulableLookaheadPodsCount.WithLabelValues(ekName)
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), val)

	pm.UpdateResizableVmUnschedulableLookaheadPodsCount(e4aName, 3)

	gauge = resizableVmUnschedulableLookaheadPodsCount.WithLabelValues(e4aName)
	val, err = testutil.GetGaugeMetricValue(gauge)
	assert.NoError(t, err)
	assert.Equal(t, float64(3), val)
}
